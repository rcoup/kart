from pathlib import Path


class WorkingCopy:
    @classmethod
    def open(cls, repo):
        repo_cfg = repo.config
        if "sno.workingcopy.version" in repo_cfg:
            version = repo_cfg["sno.workingcopy.version"]
            if repo_cfg.get_int("sno.workingcopy.version") != 1:
                raise NotImplementedError(f"Working copy version: {version}")

            path = repo_cfg["sno.workingcopy.path"]

            if path.startswith("postgresql://"):
                from .postgis import WorkingCopyPostgis

                return WorkingCopyPostgis(repo, path)
            else:
                from .gpkg import WorkingCopy_GPKG_1

                return WorkingCopy_GPKG_1.open(repo, path)

        else:
            return None

    @classmethod
    def new(cls, repo, path, version=1, **kwargs):
        if path.startswith("postgresql://"):
            from .postgis import WorkingCopyPostgis

            return WorkingCopyPostgis(repo, path, **kwargs)

        elif path.startswith("gpkg://"):
            f_path = path.split("://", 1)[1]
            if (Path(repo.path) / f_path).exists():
                raise FileExistsError(path)

            from .gpkg import WorkingCopy_GPKG_1

            return WorkingCopy_GPKG_1(repo, path, **kwargs)

        else:
            raise ValueError("Expected working copy scheme prefix")

    class Mismatch(ValueError):
        def __init__(self, working_copy_tree_id, match_tree_id):
            self.working_copy_tree_id = working_copy_tree_id
            self.match_tree_id = match_tree_id

        def __str__(self):
            return f"Working Copy is tree {self.working_copy_tree_id}; expecting {self.match_tree_id}"
