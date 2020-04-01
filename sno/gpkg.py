import collections
import hashlib
import logging
import math
import os
import re
import struct

import apsw
from osgeo import ogr, osr

from sno import spatialite_path


def ident(identifier):
    """ Sqlite identifier replacement """
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def param_str(value):
    """
    Sqlite parameter string replacement.

    Generally don't use this. Needed for creating triggers/etc though.
    """
    if value is None:
        return "NULL"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


class Row(tuple):
    def __new__(cls, cursor, row):
        return super(Row, cls).__new__(cls, row)

    def __init__(self, cursor, row):
        self._desc = tuple(d for d, _ in cursor.getdescription())

    def keys(self):
        return tuple(self._desc)

    def items(self):
        return ((k, super().__getitem__(i)) for i, k in enumerate(self._desc))

    def values(self):
        return self

    def __getitem__(self, key):
        if isinstance(key, str):
            try:
                i = self._desc.index(key)
                return super().__getitem__(i)
            except ValueError:
                raise KeyError(key)
        else:
            return super().__getitem__(key)


def _get_db_id(db):
    return hashlib.sha1(db.sqlite3pointer().to_bytes(8, "little")).hexdigest()[:4]


def get_committrace(logger):
    def _commithook():
        logger.debug("COMMIT")
        return 0


def get_exectrace(logger):
    def _trunc(value):
        if isinstance(value, str):
            return (value[:47] + "...") if len(value) > 50 else value
        elif isinstance(value, bytes):
            return (value[:47] + b"...") if len(value) > 50 else value
        return value

    def _exectrace(cursor, sql, bindings):
        if hasattr(bindings, "keys") and hasattr(bindings, "__getitem__"):  # mapping
            lb = {k: _trunc(v) for (k, v) in bindings.items()}
        elif bindings is None:  # no bindings (eg: 'COMMIT')
            lb = ""
        else:  # sequence
            lb = tuple(_trunc(b) for b in bindings)

        if isinstance(cursor, apsw.Connection):
            lc = None
        else:
            lc = id(cursor)

        logger.debug(
            "SQL (%s): %s : %s",
            lc,
            re.sub(r"\s{2,}", " ", sql).strip(),  # collapse whitespace
            lb,
        )
        return True

    return _exectrace


def db(path, **kwargs):
    db = apsw.Connection(str(path), **kwargs)

    if "_SNO_SQLITE_TRACE" in os.environ:
        L = logging.getLogger(f"sno.gpkg.sqlite_trace.{_get_db_id(db)}")
        L.info("Connection: %s", path)
        db.setexectrace(get_exectrace(L))
        db.setcommithook(get_committrace(L))

    db.setrowtrace(Row)
    dbcur = db.cursor()
    dbcur.execute("PRAGMA journal_mode = WAL;")
    dbcur.execute("PRAGMA foreign_keys = ON;")
    # current_journal = dbcur.execute("PRAGMA journal_mode").fetchone()[0]
    # if current_journal.lower() == "delete":
    #     dbcur.execute("PRAGMA journal_mode = TRUNCATE;")  # faster

    db.enableloadextension(True)
    dbcur.execute("SELECT load_extension(?)", (spatialite_path,))
    dbcur.execute("SELECT EnableGpkgMode();")
    del dbcur
    return db


def get_meta_info(db, layer, repo_version="0.0.1"):
    yield ("version", {"version": repo_version})

    dbcur = db.cursor()
    table = layer

    QUERIES = {
        "gpkg_contents": (
            # we ignore dynamic fields (last-change, min_x, min_y, max_x, max_y)
            f"SELECT table_name, data_type, identifier, description, srs_id FROM gpkg_contents WHERE table_name=?;",
            (table,),
            dict,
        ),
        "gpkg_geometry_columns": (
            f"SELECT table_name, column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name=?;",
            (table,),
            dict,
        ),
        "sqlite_table_info": (f"PRAGMA table_info({ident(table)});", (), list),
        "gpkg_metadata_reference": (
            """
            SELECT MR.*
            FROM gpkg_metadata_reference MR
                INNER JOIN gpkg_metadata M ON (MR.md_file_id = M.id)
            WHERE
                MR.table_name=?
                AND MR.column_name IS NULL
                AND MR.row_id_value IS NULL;
            """,
            (table,),
            list,
        ),
        "gpkg_metadata": (
            """
            SELECT M.*
            FROM gpkg_metadata_reference MR
                INNER JOIN gpkg_metadata M ON (MR.md_file_id = M.id)
            WHERE
                MR.table_name=?
                AND MR.column_name IS NULL
                AND MR.row_id_value IS NULL;
            """,
            (table,),
            list,
        ),
        "gpkg_spatial_ref_sys": (
            """
            SELECT DISTINCT SRS.*
            FROM gpkg_spatial_ref_sys SRS
                LEFT OUTER JOIN gpkg_contents C ON (C.srs_id = SRS.srs_id)
                LEFT OUTER JOIN gpkg_geometry_columns G ON (G.srs_id = SRS.srs_id)
            WHERE
                (C.table_name=? OR G.table_name=?)
            """,
            (table, table),
            list,
        ),
    }
    try:
        for filename, (sql, params, rtype) in QUERIES.items():
            # check table exists, the metadata ones may not
            if not filename.startswith("sqlite_"):
                dbcur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                    (filename,),
                )
                if not dbcur.fetchone():
                    continue

            dbcur.execute(sql, params)
            value = [
                collections.OrderedDict(sorted(zip(row.keys(), row))) for row in dbcur
            ]
            if rtype is dict:
                value = value[0] if len(value) else None
            yield (filename, value)
    except Exception:
        print(f"Error building meta/{filename}")
        raise


def pk(db, table):
    """ Find the primary key for a GeoPackage table """

    # Requirement 150:
    # A feature table or view SHALL have a column that uniquely identifies the
    # row. For a feature table, the column SHOULD be a primary key. If there
    # is no primary key column, the first column SHALL be of type INTEGER and
    # SHALL contain unique values for each row.

    q = db.cursor().execute(f"PRAGMA table_info({ident(table)});")
    fields = []
    for field in q:
        if field["pk"]:
            return field["name"]
        fields.append(field)

    if fields[0]["type"] == "INTEGER":
        return fields[0]["name"]
    else:
        raise ValueError("No valid GeoPackage primary key field found")


def geom_cols(db, table):
    q = db.cursor().execute(
        """
            SELECT column_name
            FROM gpkg_geometry_columns
            WHERE table_name=?
            ORDER BY column_name;
        """,
        (table,),
    )
    return tuple(r[0] for r in q)


def _bo(is_le):
    return "<" if is_le else ">"


class GPKGGeometry(bytes):
    pass


def parse_gpkg_geom(gpkg_geom):
    if not isinstance(gpkg_geom, bytes):
        raise TypeError("Expected bytes")

    if gpkg_geom[0:2] != b"GP":  # 0x4750
        raise ValueError("Expected GeoPackage Binary Geometry")
    (version, flags) = struct.unpack_from("BB", gpkg_geom, 2)
    if version != 0:
        raise NotImplementedError("Expected GeoPackage v1 geometry, got %d", version)

    is_le = (flags & 0b0000001) != 0  # Endian-ness

    if flags & (0b00100000):  # GeoPackageBinary type
        raise NotImplementedError("ExtendedGeoPackageBinary")

    envelope_typ = (flags & 0b000001110) >> 1
    wkb_offset = 8
    if envelope_typ == 1:
        wkb_offset += 32
    elif envelope_typ in (2, 3):
        wkb_offset += 48
    elif envelope_typ == 4:
        wkb_offset += 64
    elif envelope_typ == 0:
        pass
    else:
        raise ValueError("Invalid envelope contents indicator")

    srid = struct.unpack_from(f"{_bo(is_le)}i", gpkg_geom, 4)[0]

    return wkb_offset, is_le, srid


def geom_to_ogr(gpkg_geom, parse_srs=False):
    """
    Parse GeoPackage geometry values to an OGR Geometry object
    http://www.geopackage.org/spec/#gpb_format
    """
    if gpkg_geom is None:
        return None

    wkb_offset, is_le, srid = parse_gpkg_geom(gpkg_geom)

    geom = ogr.CreateGeometryFromWkb(gpkg_geom[wkb_offset:])
    assert geom is not None

    if parse_srs and srid > 0:
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(srid)
        geom.AssignSpatialReference(srs)

    return geom


def geom_to_ewkb(gpkg_geom):
    """
    Parse GeoPackage geometry values to a PostGIS EWKB value
    http://www.geopackage.org/spec/#gpb_format
    """
    if gpkg_geom is None:
        return None

    wkb_offset, is_le, srid = parse_gpkg_geom(gpkg_geom)

    wkb_is_le = struct.unpack_from("B", gpkg_geom, wkb_offset)[0]
    bo = _bo(wkb_is_le)

    wkb_type = struct.unpack_from(f"{bo}I", gpkg_geom, wkb_offset + 1)[0]
    wkb_geom_type = (wkb_type & 0xFFFF) % 1000
    iso_zm = (wkb_type & 0xFFFF) // 1000
    has_z = iso_zm in (1, 3)
    has_m = iso_zm in (2, 3)

    ewkb_geom_type = wkb_geom_type
    ewkb_geom_type |= 0x80000000 * has_z
    ewkb_geom_type |= 0x40000000 * has_m
    ewkb_geom_type |= 0x20000000 * (srid > 0)

    ewkb = struct.pack(f"{bo}BI", int(wkb_is_le), ewkb_geom_type)

    if srid > 0:
        ewkb += struct.pack(f"{bo}I", srid)

    ewkb += gpkg_geom[(wkb_offset + 5) :]

    return ewkb


def hexewkb_to_geom(hexewkb):
    """
    Parse PostGIS Hex EWKB to GeoPackage geometry
    https://github.com/postgis/postgis/blob/master/doc/ZMSgeoms.txt
    """
    if hexewkb is None:
        return None

    ewkb = bytes.fromhex(hexewkb)
    is_le = struct.unpack_from("B", ewkb)[0]
    bo = _bo(is_le)

    ewkb_type = struct.unpack_from(f"{bo}I", ewkb, 1)[0]
    has_z = bool(ewkb_type & 0x80000000)
    has_m = bool(ewkb_type & 0x40000000)
    has_srid = bool(ewkb_type & 0x20000000)

    geom_type = ewkb_type & 0xFFFF
    wkb_type = geom_type + 1000 * has_z + 2000 * has_m

    data_offset = 5
    if has_srid:
        srid = struct.unpack_from(f"{bo}I", ewkb, data_offset)[0]
        data_offset += 4
    else:
        srid = 0

    if wkb_type % 1000 == 1:
        # detect POINT[ZM] EMPTY
        px, py = struct.unpack_from(f"{bo}dd", ewkb, data_offset)
        is_empty = math.isnan(px) and math.isnan(py)
    else:
        wkb_num = struct.unpack_from(f"{bo}I", ewkb, data_offset)[
            0
        ]  # num(Points|Rings|Polygons|...)
        is_empty = wkb_num == 0

    flags = 0
    if is_le:
        flags |= 1
    if is_empty:
        flags |= 0b00010000

    gpkg_geom = (
        struct.pack(
            f"{bo}ccBBiBI", b"G", b"P", 0, flags, srid, int(is_le), wkb_type,  # version
        )
        + ewkb[data_offset:]
    )

    return gpkg_geom


def geom_envelope(gpkg_geom):
    """
    Parse GeoPackage geometry to a 2D envelope.
    This is a shortcut to avoid instantiating a full OGR geometry if possible.

    Returns a 4-tuple (minx, maxx, miny, maxy), or None if the geometry is empty.

    http://www.geopackage.org/spec/#gpb_format
    """
    if gpkg_geom is None:
        return None

    if not isinstance(gpkg_geom, bytes):
        raise TypeError("Expected bytes")

    if gpkg_geom[0:2] != b"GP":  # 0x4750
        raise ValueError("Expected GeoPackage Binary Geometry")
    (version, flags) = struct.unpack_from("BB", gpkg_geom, 2)
    if version != 0:
        raise NotImplementedError("Expected GeoPackage v1 geometry, got %d", version)

    is_le = (flags & 0b0000001) != 0  # Endian-ness

    if flags & (0b00100000):  # GeoPackageBinary type
        raise NotImplementedError("ExtendedGeoPackageBinary")

    if flags & (0b00010000):  # Empty geometry
        return None

    envelope_typ = (flags & 0b000001110) >> 1
    # E: envelope contents indicator code (3-bit unsigned integer)
    # 0: no envelope (space saving slower indexing option), 0 bytes
    # 1: envelope is [minx, maxx, miny, maxy], 32 bytes
    # 2: envelope is [minx, maxx, miny, maxy, minz, maxz], 48 bytes
    # 3: envelope is [minx, maxx, miny, maxy, minm, maxm], 48 bytes
    # 4: envelope is [minx, maxx, miny, maxy, minz, maxz, minm, maxm], 64 bytes
    # 5-7: invalid

    if envelope_typ == 0:
        # parse the full geometry then get it's envelope
        return geom_to_ogr(gpkg_geom).GetEnvelope()
    elif envelope_typ <= 4:
        # we only care about 2D envelopes here
        envelope = struct.unpack_from(f"{'<' if is_le else '>'}dddd", gpkg_geom, 8)
        if any(math.isnan(c) for c in envelope):
            return None
        else:
            return envelope
    else:
        raise ValueError("Invalid envelope contents indicator")
