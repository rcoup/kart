import functools
import re
import shutil

from kart.core import find_blobs_in_tree
from kart.base_dataset import BaseDataset, MetaItemDefinition, MetaItemFileType
from kart.diff_structs import DatasetDiff, DeltaDiff, Delta, KeyValue
from kart.key_filters import DatasetKeyFilter, FeatureKeyFilter
from kart.list_of_conflicts import ListOfConflicts, InvalidNewValue
from kart.lfs_util import (
    copy_file_to_local_lfs_cache,
    get_hash_and_size_of_file,
    get_hash_from_pointer_file,
    get_local_path_from_lfs_hash,
    pointer_file_bytes_to_dict,
    dict_to_pointer_file_bytes,
)
from kart.point_cloud.metadata_util import (
    RewriteMetadata,
    extract_pc_tile_metadata,
    rewrite_and_merge_metadata,
    format_tile_for_pointer_file,
    get_format_summary,
    set_file_extension,
    remove_las_extension,
)
from kart.point_cloud.pdal_convert import convert_tile_to_format
from kart.serialise_util import hexhash
from kart.working_copy import PartType


class PointCloudV1(BaseDataset):
    """A V1 point-cloud (LIDAR) dataset."""

    VERSION = 1
    DATASET_TYPE = "point-cloud"
    DATASET_DIRNAME = ".point-cloud-dataset.v1"

    WORKING_COPY_PART_TYPE = PartType.WORKDIR

    # All relative paths should be relative to self.inner_tree - that is, to the tree named DATASET_DIRNAME.
    TILE_PATH = "tile/"

    FORMAT_JSON = MetaItemDefinition("format.json", MetaItemFileType.JSON)

    META_ITEMS = (
        BaseDataset.TITLE,
        BaseDataset.DESCRIPTION,
        BaseDataset.METADATA_XML,
        FORMAT_JSON,
        BaseDataset.SCHEMA_JSON,
        BaseDataset.CRS_WKT,
    )

    @property
    def tile_tree(self):
        return self.get_subtree(self.TILE_PATH)

    def tile_pointer_blobs(self):
        """Returns a generator that yields every tile pointer blob in turn."""
        tile_tree = self.tile_tree
        if tile_tree:
            yield from find_blobs_in_tree(tile_tree)

    @property
    def tile_count(self):
        """The total number of features in this dataset."""
        return sum(1 for blob in self.tile_pointer_blobs())

    def tilenames_with_lfs_hashes(self):
        """Returns a generator that yields every tilename along with its LFS hash."""
        for blob in self.tile_pointer_blobs():
            yield blob.name, get_hash_from_pointer_file(blob)

    def tilenames_with_lfs_paths(self):
        """Returns a generator that yields every tilename along with the path where the tile content is stored locally."""
        for blob_name, lfs_hash in self.tilenames_with_lfs_hashes():
            yield blob_name, get_local_path_from_lfs_hash(self.repo, lfs_hash)

    def decode_path(self, path):
        rel_path = self.ensure_rel_path(path)
        if rel_path.startswith("tile/"):
            return ("tile", self.tilename_from_path(rel_path))
        return super().decode_path(rel_path)

    def tilename_to_blob_path(self, tilename, relative=False):
        """Given a tile's name, returns the path the tile's pointer should be written to."""
        tilename = self.tilename_from_path(
            tilename
        )  # Just in case it's a whole path, not just a name.
        tile_prefix = hexhash(tilename)[0:2]
        rel_path = f"tile/{tile_prefix}/{tilename}"
        return rel_path if relative else self.ensure_full_path(rel_path)

    def tilename_to_working_copy_path(self, tilename):
        # Just in case it's a whole path, not just a name.
        tilename = self.tilename_from_path(tilename)
        return f"{self.path}/{tilename}"

    @classmethod
    def tilename_from_path(cls, tile_path):
        return tile_path.rsplit("/", maxsplit=1)[-1]

    def get_tile_summary_from_pointer_blob(self, tile_pointer_blob):
        result = pointer_file_bytes_to_dict(
            tile_pointer_blob, {"name": tile_pointer_blob.name}
        )
        if "version" in result:
            del result["version"]
        return result

    def _workdir_path(self, wc_path):
        if isinstance(wc_path, str):
            return self.repo.workdir_file(wc_path)
        else:
            return wc_path

    def get_tile_summary_from_wc_path(self, wc_path):
        wc_path = self._workdir_path(wc_path)

        return self.get_tile_summary_from_pc_tile_metadata(
            wc_path, extract_pc_tile_metadata(wc_path)
        )

    def get_tile_summary_promise_from_wc_path(self, wc_path):
        return functools.partial(self.get_tile_summary_from_wc_path, wc_path)

    def get_tile_summary_from_pc_tile_metadata(self, wc_path, tile_metadata):
        wc_path = self._workdir_path(wc_path)

        tile_info = format_tile_for_pointer_file(tile_metadata["tile"])
        oid, size = get_hash_and_size_of_file(wc_path)
        return {"name": wc_path.name, **tile_info, "oid": f"sha256:{oid}", "size": size}

    def diff(self, other, ds_filter=DatasetKeyFilter.MATCH_ALL, reverse=False):
        """
        Generates a Diff from self -> other.
        If reverse is true, generates a diff from other -> self.
        """
        ds_diff = super().diff(other, ds_filter=ds_filter, reverse=reverse)
        tile_filter = ds_filter.get("tile", ds_filter.child_type())
        ds_diff["tile"] = DeltaDiff(self.diff_tile(other, tile_filter, reverse=reverse))
        return ds_diff

    def diff_tile(self, other, tile_filter=FeatureKeyFilter.MATCH_ALL, reverse=False):
        """
        Yields tile deltas from self -> other, but only for tile that match the tile_filter.
        If reverse is true, yields tile deltas from other -> self.
        """
        yield from self.diff_subtree(
            other,
            "tile",
            key_filter=tile_filter,
            key_decoder_method="tilename_from_path",
            value_decoder_method="get_tile_summary_promise_from_path",
            reverse=reverse,
        )

    def get_tile_summary_promise_from_path(self, tile_path):
        tile_pointer_blob = self.get_blob_at(tile_path)
        return functools.partial(
            self.get_tile_summary_from_pointer_blob, tile_pointer_blob
        )

    def diff_to_working_copy(
        self,
        workdir_diff_cache,
        ds_filter=DatasetKeyFilter.MATCH_ALL,
        *,
        convert_to_dataset_format=False,
    ):
        """Returns a diff of all changes made to this dataset in the working copy."""
        tile_filter = ds_filter.get("tile", ds_filter.child_type())

        current_metadata = self.tile_metadata
        dataset_format_to_apply = None
        if convert_to_dataset_format:
            dataset_format_to_apply = get_format_summary(current_metadata["format"])

        tilename_to_metadata = {}

        wc_tiles_path_pattern = re.escape(f"{self.path}/")
        wc_tile_ext_pattern = r"\.[Ll][Aa][SsZz]"
        wc_tiles_pattern = re.compile(
            rf"^{wc_tiles_path_pattern}[^/]+{wc_tile_ext_pattern}$"
        )

        def wc_to_ds_path_transform(wc_path):
            return self.tilename_to_blob_path(wc_path, relative=True)

        def tile_summary_from_wc_path(wc_path):
            wc_path = self._workdir_path(wc_path)
            tile_metadata = extract_pc_tile_metadata(wc_path)
            tilename_to_metadata[wc_path.name] = tile_metadata
            tile_summary = self.get_tile_summary_from_pc_tile_metadata(
                wc_path, tile_metadata
            )
            if dataset_format_to_apply and not self.is_tile_compatible(
                dataset_format_to_apply, tile_summary
            ):
                tile_summary = self.pre_conversion_tile_summary(
                    dataset_format_to_apply, tile_summary
                )
            return tile_summary

        tile_diff_deltas = self.generate_wc_diff_from_workdir_index(
            workdir_diff_cache,
            wc_path_filter_pattern=wc_tiles_pattern,
            key_filter=tile_filter,
            wc_to_ds_path_transform=wc_to_ds_path_transform,
            ds_key_decoder=self.tilename_from_path,
            wc_key_decoder=self.tilename_from_path,
            ds_value_decoder=self.get_tile_summary_promise_from_path,
            wc_value_decoder=tile_summary_from_wc_path,
        )
        tile_diff = DeltaDiff(tile_diff_deltas)

        if not tile_diff:
            return DatasetDiff()

        is_clean_slate = self.is_clean_slate(tile_diff)
        metadata_list = list(tilename_to_metadata.values())
        no_new_metadata = not metadata_list

        if not is_clean_slate:
            metadata_list.insert(0, current_metadata)

        rewrite_metadata = 0
        optimization_constraint = current_metadata["format"].get("optimization")
        if convert_to_dataset_format:
            rewrite_metadata = (
                RewriteMetadata.AS_IF_CONVERTED_TO_COPC
                if optimization_constraint == "copc"
                else RewriteMetadata.DROP_FORMAT
            )
        else:
            rewrite_metadata = (
                0
                if optimization_constraint == "copc"
                else RewriteMetadata.DROP_OPTIMIZATION
            )

        if no_new_metadata:
            merged_metadata = current_metadata
        else:
            merged_metadata = rewrite_and_merge_metadata(
                metadata_list, rewrite_metadata
            )
            if rewrite_metadata & RewriteMetadata.DROP_FORMAT:
                merged_metadata["format"] = current_metadata["format"]

        # Make it invalid to try and commit and LAS files:
        merged_format = merged_metadata["format"]
        if (
            not isinstance(merged_format, ListOfConflicts)
            and merged_format.get("compression") == "las"
        ):
            merged_format = InvalidNewValue([merged_format])
            merged_format.error_message = "Committing LAS tiles is not supported, unless you specify the --convert-to-dataset-format flag"
            merged_metadata["format"] = merged_format

        meta_diff = DeltaDiff()
        for key, ext in (("format", "json"), ("schema", "json"), ("crs", "wkt")):
            if current_metadata[key] != merged_metadata[key]:
                item_name = f"{key}.{ext}"
                meta_diff[item_name] = Delta.update(
                    KeyValue.of((item_name, current_metadata[key])),
                    KeyValue.of((item_name, merged_metadata[key])),
                )

        ds_diff = DatasetDiff()
        ds_diff["meta"] = meta_diff
        ds_diff["tile"] = tile_diff

        return ds_diff

    def is_tile_compatible(self, ds_format, tile_summary):
        tile_format = tile_summary["format"]
        if isinstance(ds_format, dict):
            ds_format = get_format_summary(ds_format)
        return tile_format == ds_format or tile_format.startswith(f"{ds_format}/")

    def pre_conversion_tile_summary(self, ds_format, tile_summary):
        """
        Converts a tile-summary - that is, updates the tile-summary to be a mix of the tiles current information
        (prefixed with "source") and its future information - what it will be once converted - where that is known.
        """
        if isinstance(ds_format, dict):
            ds_format = get_format_summary(ds_format)

        envisioned_summary = {
            "name": set_file_extension(tile_summary["name"], tile_format=ds_format),
            "format": ds_format,
            "oid": None,
            "size": None,
        }
        result = {}
        for key, value in tile_summary.items():
            if envisioned_summary.get(key):
                result[key] = envisioned_summary[key]
            if key in envisioned_summary:
                result["source" + key[0].upper() + key[1:]] = value
            else:
                result[key] = value
        return result

    def is_clean_slate(self, tile_diff):
        num_existing_tiles_kept = self.tile_count
        for tile_delta in tile_diff.values():
            if tile_delta.type != "insert":
                num_existing_tiles_kept -= 1
        return num_existing_tiles_kept == 0

    def apply_diff(
        self, dataset_diff, object_builder, *, resolve_missing_values_from_ds=None
    ):
        """
        Given a diff that only affects this dataset, write it to the given treebuilder.
        Blobs will be created in the repo, and referenced in the resulting tree, but
        no commit is created - this is the responsibility of the caller.
        """
        meta_diff = dataset_diff.get("meta")
        if meta_diff:
            self.apply_meta_diff(
                meta_diff,
                object_builder,
                resolve_missing_values_from_ds=resolve_missing_values_from_ds,
            )

        tile_diff = dataset_diff.get("tile")
        if tile_diff:
            self.apply_tile_diff(
                tile_diff,
                object_builder,
                resolve_missing_values_from_ds=resolve_missing_values_from_ds,
            )

    def apply_tile_diff(
        self, tile_diff, object_builder, *, resolve_missing_values_from_ds=None
    ):
        lfs_objects_path = self.repo.gitdir_path / "lfs" / "objects"
        lfs_tmp_path = lfs_objects_path / "tmp"
        lfs_tmp_path.mkdir(parents=True, exist_ok=True)

        with object_builder.chdir(self.inner_path):
            for delta in tile_diff.values():

                if delta.type in ("insert", "update"):
                    # TODO - need more work on normalising / matching names with different extensions

                    if delta.new_value.get("sourceFormat"):
                        # Converting and then committing a new tile
                        source_name = delta.new_value.get("sourceName")
                        path_in_wc = self._workdir_path(f"{self.path}/{source_name}")

                        conversion_func = functools.partial(
                            convert_tile_to_format,
                            target_format=delta.new_value["format"],
                        )
                        pointer_dict = copy_file_to_local_lfs_cache(
                            self.repo, path_in_wc, conversion_func
                        )
                        pointer_dict = format_tile_for_pointer_file(
                            delta.new_value, pointer_dict
                        )
                    else:
                        # Committing in a new tile, preserving its format
                        source_name = delta.new_value.get("name")
                        path_in_wc = self._workdir_path(f"{self.path}/{source_name}")
                        oid = delta.new_value["oid"]
                        path_in_lfs_cache = get_local_path_from_lfs_hash(self.repo, oid)
                        path_in_lfs_cache.parents[0].mkdir(parents=True, exist_ok=True)
                        shutil.copy(path_in_wc, path_in_lfs_cache)
                        pointer_dict = format_tile_for_pointer_file(delta.new_value)

                    tilename = delta.new_value["name"]
                    object_builder.insert(
                        self.tilename_to_blob_path(tilename, relative=True),
                        dict_to_pointer_file_bytes(pointer_dict),
                    )

                else:  # delete:
                    tilename = delta.old_key
                    object_builder.remove(
                        self.tilename_to_blob_path(tilename, relative=True)
                    )

    def find_tile_in_wc(self, tilename):
        """
        Finds a tile by name in the working directory.
        Searches for the tile using multiple extensions, ie .las or .laz or .copc.laz
        """
        tilename = remove_las_extension(tilename)
        wc_folder = self.repo.workdir_file(self.path)
        for file in wc_folder.glob(tilename + ".*"):
            if remove_las_extension(file.name) == tilename:
                return file
        raise RuntimeError(f"Couldn't find {tilename} in workdir")

    @property
    def tile_metadata(self):
        return {
            "format": self.get_meta_item("format.json"),
            "schema": self.get_meta_item("schema.json"),
            "crs": self.get_meta_item("crs.wkt"),
        }
