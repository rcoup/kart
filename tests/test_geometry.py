import re

import psycopg2
from osgeo import ogr, osr

import pytest

from sno.gpkg import geom_to_ogr, geom_to_ewkb, hexewkb_to_geom, parse_gpkg_geom


# Sourced from:
# https://github.com/OSGeo/gdal/blob/master/autotest/ogr/ogr_wkbwkt_geom.py
WKT = [
    # ('POINT EMPTY', 'POINT EMPTY'),
    # ('POINT Z EMPTY', 'POINT EMPTY'),
    # ('POINT M EMPTY', 'POINT EMPTY'),
    # ('POINT ZM EMPTY', 'POINT EMPTY'),
    ("POINT (0 1)", "POINT (0 1)"),
    ("POINT Z (0 1 2)", "POINT (0 1 2)"),
    ("POINT M (0 1 2)", "POINT (0 1)"),
    ("POINT ZM (0 1 2 3)", "POINT (0 1 2)"),
    # ('LINESTRING EMPTY', 'LINESTRING EMPTY'),
    # ('LINESTRING Z EMPTY', 'LINESTRING EMPTY'),
    # ('LINESTRING M EMPTY', 'LINESTRING EMPTY'),
    # ('LINESTRING ZM EMPTY', 'LINESTRING EMPTY'),
    ("LINESTRING (0 1,2 3)", "LINESTRING (0 1,2 3)"),
    ("LINESTRING Z (0 1 2,3 4 5)", "LINESTRING (0 1 2,3 4 5)"),
    ("LINESTRING M (0 1 2,3 4 5)", "LINESTRING (0 1,3 4)"),
    ("LINESTRING ZM (0 1 2 3,4 5 6 7)", "LINESTRING (0 1 2,4 5 6)"),
    # ('POLYGON EMPTY', 'POLYGON EMPTY'),
    # ('POLYGON (EMPTY)', 'POLYGON EMPTY'),
    # ('POLYGON Z EMPTY', 'POLYGON EMPTY'),
    # ('POLYGON Z (EMPTY)', 'POLYGON EMPTY'),
    # ('POLYGON M EMPTY', 'POLYGON EMPTY'),
    # ('POLYGON ZM EMPTY', 'POLYGON EMPTY'),
    ("POLYGON ((0 1,2 3,4 5,0 1))", "POLYGON ((0 1,2 3,4 5,0 1))"),
    # ('POLYGON ((0 1,2 3,4 5,0 1),EMPTY)', 'POLYGON ((0 1,2 3,4 5,0 1))'),
    # ('POLYGON (EMPTY,(0 1,2 3,4 5,0 1))', 'POLYGON EMPTY'),
    # ('POLYGON (EMPTY,(0 1,2 3,4 5,0 1),EMPTY)', 'POLYGON EMPTY'),
    (
        "POLYGON Z ((0 1 10,2 3 20,4 5 30,0 1 10),(0 1 10,2 3 20,4 5 30,0 1 10))",
        "POLYGON ((0 1 10,2 3 20,4 5 30,0 1 10),(0 1 10,2 3 20,4 5 30,0 1 10))",
    ),
    ("POLYGON M ((0 1 10,2 3 20,4 5 30,0 1 10))", "POLYGON ((0 1,2 3,4 5,0 1))"),
    (
        "POLYGON ZM ((0 1 10 100,2 3 20 200,4 5 30 300,0 1 10 10))",
        "POLYGON ((0 1 10,2 3 20,4 5 30,0 1 10))",
    ),
    # ('MULTIPOINT EMPTY', 'MULTIPOINT EMPTY'),
    # ('MULTIPOINT (EMPTY)', 'MULTIPOINT EMPTY'),
    # ('MULTIPOINT Z EMPTY', 'MULTIPOINT EMPTY'),
    # ('MULTIPOINT Z (EMPTY)', 'MULTIPOINT EMPTY'),
    # ('MULTIPOINT M EMPTY', 'MULTIPOINT EMPTY'),
    # ('MULTIPOINT ZM EMPTY', 'MULTIPOINT EMPTY'),
    ("MULTIPOINT ((0 1),(2 3))", "MULTIPOINT (0 1,2 3)"),
    # ('MULTIPOINT ((0 1),EMPTY)', 'MULTIPOINT (0 1)'),  # We don't output empty points in multipoint
    # ('MULTIPOINT (EMPTY,(0 1))', 'MULTIPOINT (0 1)'),  # We don't output empty points in multipoint
    # ('MULTIPOINT (EMPTY,(0 1),EMPTY)', 'MULTIPOINT (0 1)'),  # We don't output empty points in multipoint
    ("MULTIPOINT Z ((0 1 2),(3 4 5))", "MULTIPOINT (0 1 2,3 4 5)"),
    ("MULTIPOINT M ((0 1 2),(3 4 5))", "MULTIPOINT (0 1,3 4)"),
    ("MULTIPOINT ZM ((0 1 2 3),(4 5 6 7))", "MULTIPOINT (0 1 2,4 5 6)"),
    # ('MULTILINESTRING EMPTY', 'MULTILINESTRING EMPTY'),
    # ('MULTILINESTRING (EMPTY)', 'MULTILINESTRING EMPTY'),
    # ('MULTILINESTRING Z EMPTY', 'MULTILINESTRING EMPTY'),
    # ('MULTILINESTRING Z (EMPTY)', 'MULTILINESTRING EMPTY'),
    # ('MULTILINESTRING M EMPTY', 'MULTILINESTRING EMPTY'),
    # ('MULTILINESTRING ZM EMPTY', 'MULTILINESTRING EMPTY'),
    ("MULTILINESTRING ((0 1,2 3,4 5,0 1))", "MULTILINESTRING ((0 1,2 3,4 5,0 1))"),
    # ('MULTILINESTRING ((0 1,2 3,4 5,0 1),EMPTY)', 'MULTILINESTRING ((0 1,2 3,4 5,0 1))'),
    # ('MULTILINESTRING (EMPTY,(0 1,2 3,4 5,0 1))', 'MULTILINESTRING ((0 1,2 3,4 5,0 1))'),
    # ('MULTILINESTRING (EMPTY,(0 1,2 3,4 5,0 1),EMPTY)', 'MULTILINESTRING ((0 1,2 3,4 5,0 1))'),
    (
        "MULTILINESTRING Z ((0 1 10,2 3 20,4 5 30,0 1 10),(0 1 10,2 3 20,4 5 30,0 1 10))",
        "MULTILINESTRING ((0 1 10,2 3 20,4 5 30,0 1 10),(0 1 10,2 3 20,4 5 30,0 1 10))",
    ),
    (
        "MULTILINESTRING M ((0 1 10,2 3 20,4 5 30,0 1 10))",
        "MULTILINESTRING ((0 1,2 3,4 5,0 1))",
    ),
    (
        "MULTILINESTRING ZM ((0 1 10 100,2 3 20 200,4 5 30 300,0 1 10 10))",
        "MULTILINESTRING ((0 1 10,2 3 20,4 5 30,0 1 10))",
    ),
    # ('MULTIPOLYGON EMPTY', 'MULTIPOLYGON EMPTY'),
    # ('MULTIPOLYGON (EMPTY)', 'MULTIPOLYGON EMPTY'),
    # ('MULTIPOLYGON Z EMPTY', 'MULTIPOLYGON EMPTY'),
    # ('MULTIPOLYGON Z (EMPTY)', 'MULTIPOLYGON EMPTY'),
    # ('MULTIPOLYGON M EMPTY', 'MULTIPOLYGON EMPTY'),
    # ('MULTIPOLYGON ZM EMPTY', 'MULTIPOLYGON EMPTY'),
    # ('MULTIPOLYGON ((EMPTY))', 'MULTIPOLYGON EMPTY'),
    ("MULTIPOLYGON (((0 1,2 3,4 5,0 1)))", "MULTIPOLYGON (((0 1,2 3,4 5,0 1)))"),
    (
        "MULTIPOLYGON (((0 1,2 3,4 5,0 1)),((2 3,4 5,6 7,2 3)))",
        "MULTIPOLYGON (((0 1,2 3,4 5,0 1)),((2 3,4 5,6 7,2 3)))",
    ),
    (
        "MULTIPOLYGON (((0 1,2 3,4 5,0 1),(2 3,4 5,6 7,2 3)))",
        "MULTIPOLYGON (((0 1,2 3,4 5,0 1),(2 3,4 5,6 7,2 3)))",
    ),
    # ('MULTIPOLYGON (((0 1,2 3,4 5,0 1)),EMPTY)', 'MULTIPOLYGON (((0 1,2 3,4 5,0 1)))'),
    # ('MULTIPOLYGON (((0 1,2 3,4 5,0 1),EMPTY))', 'MULTIPOLYGON (((0 1,2 3,4 5,0 1)))'),
    # ('MULTIPOLYGON ((EMPTY,(0 1,2 3,4 5,0 1)))', 'MULTIPOLYGON EMPTY'),
    # ('MULTIPOLYGON (((0 1,2 3,4 5,0 1),EMPTY,(2 3,4 5,6 7,2 3)))', 'MULTIPOLYGON (((0 1,2 3,4 5,0 1),(2 3,4 5,6 7,2 3)))'),
    (
        "MULTIPOLYGON (((0 1,2 3,4 5,0 1)),((0 1,2 3,4 5,0 1),(2 3,4 5,6 7,2 3)))",
        "MULTIPOLYGON (((0 1,2 3,4 5,0 1)),((0 1,2 3,4 5,0 1),(2 3,4 5,6 7,2 3)))",
    ),
    # ('MULTIPOLYGON (EMPTY,((0 1,2 3,4 5,0 1)))', 'MULTIPOLYGON (((0 1,2 3,4 5,0 1)))'),
    # ('MULTIPOLYGON (((0 1,2 3,4 5,0 1)),EMPTY)', 'MULTIPOLYGON (((0 1,2 3,4 5,0 1)))'),
    (
        "MULTIPOLYGON Z (((0 1 10,2 3 20,4 5 30,0 1 10)),((0 1 10,2 3 20,4 5 30,0 1 10)))",
        "MULTIPOLYGON (((0 1 10,2 3 20,4 5 30,0 1 10)),((0 1 10,2 3 20,4 5 30,0 1 10)))",
    ),
    (
        "MULTIPOLYGON M (((0 1 10,2 3 20,4 5 30,0 1 10)))",
        "MULTIPOLYGON (((0 1,2 3,4 5,0 1)))",
    ),
    (
        "MULTIPOLYGON ZM (((0 1 10 100,2 3 20 200,4 5 30 300,0 1 10 10)))",
        "MULTIPOLYGON (((0 1 10,2 3 20,4 5 30,0 1 10)))",
    ),
    # ('GEOMETRYCOLLECTION EMPTY', 'GEOMETRYCOLLECTION EMPTY'),
    # ('GEOMETRYCOLLECTION Z EMPTY', 'GEOMETRYCOLLECTION EMPTY'),
    # ('GEOMETRYCOLLECTION M EMPTY', 'GEOMETRYCOLLECTION EMPTY'),
    # ('GEOMETRYCOLLECTION ZM EMPTY', 'GEOMETRYCOLLECTION EMPTY'),
    (
        "GEOMETRYCOLLECTION Z (POINT Z (0 1 2),LINESTRING Z (0 1 2,3 4 5))",
        "GEOMETRYCOLLECTION (POINT (0 1 2),LINESTRING (0 1 2,3 4 5))",
    ),
    (
        "GEOMETRYCOLLECTION M (POINT M (0 1 2),LINESTRING M (0 1 2,3 4 5))",
        "GEOMETRYCOLLECTION (POINT (0 1),LINESTRING (0 1,3 4))",
    ),
    (
        "GEOMETRYCOLLECTION ZM (POINT ZM (0 1 2 10),LINESTRING ZM (0 1 2 10,3 4 5 20))",
        "GEOMETRYCOLLECTION (POINT (0 1 2),LINESTRING (0 1 2,3 4 5))",
    ),
    # ('GEOMETRYCOLLECTION (POINT EMPTY,LINESTRING EMPTY,POLYGON EMPTY,MULTIPOINT EMPTY,MULTILINESTRING EMPTY,MULTIPOLYGON EMPTY,GEOMETRYCOLLECTION EMPTY)',
    #'GEOMETRYCOLLECTION (POINT EMPTY,LINESTRING EMPTY,POLYGON EMPTY,MULTIPOINT EMPTY,MULTILINESTRING EMPTY,MULTIPOLYGON EMPTY,GEOMETRYCOLLECTION EMPTY)'),
    # ('GEOMETRYCOLLECTION (POINT Z EMPTY,LINESTRING Z EMPTY,POLYGON Z EMPTY,MULTIPOINT Z EMPTY,MULTILINESTRING Z EMPTY,MULTIPOLYGON Z EMPTY,GEOMETRYCOLLECTION Z EMPTY)',
    #'GEOMETRYCOLLECTION (POINT EMPTY,LINESTRING EMPTY,POLYGON EMPTY,MULTIPOINT EMPTY,MULTILINESTRING EMPTY,MULTIPOLYGON EMPTY,GEOMETRYCOLLECTION EMPTY)'),
    # ('CURVEPOLYGON EMPTY', 'CURVEPOLYGON EMPTY'),
    # ('CURVEPOLYGON (EMPTY)', 'CURVEPOLYGON EMPTY'),
    # ('MULTICURVE EMPTY', 'MULTICURVE EMPTY'),
    # ('MULTICURVE (EMPTY)', 'MULTICURVE EMPTY'),
    # ('MULTISURFACE EMPTY', 'MULTISURFACE EMPTY'),
    # ('MULTISURFACE (EMPTY)', 'MULTISURFACE EMPTY'),
]


def assert_wkt_eq(a, b):
    assert a
    assert b

    a = re.sub(r"([A-Z ]+?) (?=\()", r"\1", a)
    b = re.sub(r"([A-Z ]+?) (?=\()", r"\1", b)

    a = re.sub(r", ", r",", a)
    b = re.sub(r", ", r",", b)

    if a.startswith("MULTIPOINT") and b.startswith("MULTIPOINT"):
        a = a.replace("))", ")").replace("),(", ",").replace("((", "(")
        b = b.replace("))", ")").replace("),(", ",").replace("((", "(")

    assert a == b


@pytest.mark.xfail
@pytest.mark.parametrize("encoding", ("NDR", "XDR"))
@pytest.mark.parametrize("srid", (0, 4167))
@pytest.mark.parametrize("wkt1,wkt2", WKT)
def test_ewkb_gpkg(wkt1, wkt2, encoding, srid, postgis_db_module, geopackage):
    pg_conn = psycopg2.connect(postgis_db_module)
    pg_cur = pg_conn.cursor()

    sl_db = geopackage()
    sl_cur = sl_db.cursor()

    # turn the WKT into EWKB in PostGIS
    # turn the WKT into GPKG geom in Spatialite
    if srid:
        pg_cur.execute(
            "SELECT ST_AsHEXEWKB(ST_SetSRID(ST_GeometryFromText(%s), %s), %s);",
            (wkt1, srid, encoding),
        )
        sl_cur.execute("SELECT AsGPB(ST_GeomFromText(?, ?))", (wkt1, srid))
    else:
        pg_cur.execute(
            "SELECT ST_AsHEXEWKB(ST_GeometryFromText(%s), %s);", (wkt1, encoding)
        )
        sl_cur.execute("SELECT AsGPB(ST_GeomFromText(?))", (wkt1,))
    ewkb_in = pg_cur.fetchone()[0]
    sl_geom = sl_cur.fetchone()[0]
    assert ewkb_in
    assert sl_geom

    if sl_geom is None:
        sl_cur.execute(
            "SELECT GEOS_GetLastWarningMsg(), GEOS_GetLastErrorMsg(), GEOS_GetLastAuxErrorMsg();"
        )
        print(
            "GeomFromText:\n GEOS_GetLastWarningMsg: {}\n GEOS_GetLastErrorMsg: {}\n GEOS_GetLastAuxErrorMsg: {}".format(
                *sl_cur.fetchone()
            )
        )

    # turn the EWKB into GPKG geometry (our code)
    gpkg_geom = hexewkb_to_geom(ewkb_in)

    # parse our own GPKG geometry (our code)
    parse_wkb_offset, parse_is_le, parse_srid = parse_gpkg_geom(gpkg_geom)
    assert parse_is_le == (encoding == "NDR")
    assert parse_srid == srid
    assert parse_wkb_offset == 8

    # check Spatialite thinks our GPKG geometry is valid
    sl_cur.execute(
        "SELECT IsValidGPB(?), AsGPB(GeomFromGPB(?))", (gpkg_geom, gpkg_geom)
    )
    sl_is_valid, sl_geom_parsed = sl_cur.fetchone()
    if sl_is_valid != 1:
        sl_cur.execute(
            "SELECT GEOS_GetLastWarningMsg(), GEOS_GetLastErrorMsg(), GEOS_GetLastAuxErrorMsg();"
        )
        print(
            "IsValidGPB:\n GEOS_GetLastWarningMsg: {}\n GEOS_GetLastErrorMsg: {}\n GEOS_GetLastAuxErrorMsg: {}".format(
                *sl_cur.fetchone()
            )
        )
    assert sl_is_valid == 1
    assert sl_geom_parsed == sl_geom

    # turn our GPKG geometry into a Spatialite one
    sl_cur.execute(
        "SELECT ST_AsText(GeomFromGPB(?)), ST_SRID(GeomFromGPB(?)), ST_IsEmpty(GeomFromGPB(?))",
        (gpkg_geom, gpkg_geom, gpkg_geom),
    )
    sl_wkt, sl_srid, sl_is_empty = sl_cur.fetchone()
    assert_wkt_eq(sl_wkt, wkt1)
    assert sl_is_empty == ("EMPTY" in wkt1)
    assert sl_srid == srid

    # parse Spatialite's GPKG geometry (our code)
    parse_sl_wkb_offset, parse_sl_is_le, parse_sl_srid = parse_gpkg_geom(sl_geom)
    assert parse_sl_is_le  # Spatialite uses LE encoding
    assert parse_sl_srid == srid
    assert parse_sl_wkb_offset > 8  # Spatialite adds envelopes

    for gg in (gpkg_geom, sl_geom):
        # Turn the GPKG geometry into OGR (our code)
        ogr_geom = geom_to_ogr(gg, parse_srs=True)
        assert ogr_geom.ExportToIsoWkt() == wkt1
        srs = ogr_geom.GetSpatialReference()
        if srid:
            assert srs is not None
            assert int(srs.GetAuthorityCode(None)) == srid
        else:
            assert srs is None

        # Turn the GPKG geometry into HEXEWKB (our code)
        ewkb_out = geom_to_ewkb(gg)

        # Check it in PostGIS
        pg_cur.execute(
            "SELECT ST_AsText(ST_GeomFromEWKB(%s)), ST_SRID(ST_GeomFromEWKB(%s));",
            (ewkb_out, ewkb_out),
        )
        wkt_out, srid_out = pg_cur.fetchone()

        assert_wkt_eq(wkt_out, wkt1)
        assert srid_out == srid
