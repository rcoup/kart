import sqlalchemy
from sqlalchemy.types import UserDefinedType


class BaseKartAdapter:
    """
    A KartAdapter adapts the Kart model (currently Datasets V2) - to or from a table in a sqlalchemy database.
    Adapts not just the features / table rows, but also other metadata such as title, description,
    CRS definitions and XML metadata (if the storage of this metadata is supported by the sqlalchemy
    database in a standardised way).
    """

    @classmethod
    def v2_schema_to_sql_spec(cls, schema):
        """
        Given a V2 schema object, returns a SQL specification that can be used with CREATE TABLE:
        For example: 'fid INTEGER, geom GEOMETRY(POINT,2136), desc VARCHAR(128), PRIMARY KEY(fid)'
        The SQL dialect and types will be conformant to the sqlalchemy database that this adapter supports.
        Some type information will be approximated if it is not fully supported by the database.

        schema - a kart.schema.Schema object.
        """

        raise NotImplementedError()

    @classmethod
    def all_v2_meta_items(cls, sess, db_schema, table_name, id_salt):
        """
        Generate all V2 meta items for the specified table, yielded as key-value pairs.
        Guaranteed to at least generate the table's V2 schema with key "schema.json".
        Possibly returns any or all of the title, description, xml metadata, and attached CRS definitions.
        Varying the id_salt varies the column ids that are generated for the schema.json item -
        these are generated deterministically so that running the same command twice in a row produces the same output.
        But if the user does something different later, a different salt should be provided.

        sess - an open sqlalchemy session.
        db_schema - the db schema (or similar) that contains the table, if any.
        table_name - the table to generate meta items for.
        id_salt - a string based on the current state that should change when the circumstances change.
        """

        raise NotImplementedError()

    @classmethod
    def table_def_for_schema(cls, schema, table_name, db_schema=None, dataset=None):
        """
        Returns a sqlalchemy table definition with conversion-logic for reading or writing data with the given schema
        to or from the given table.

        schema - a kart.schema.Schema
        table_name - the name of the table.
        db_schema - the database schema containing the table, if any.
        dataset - this is used to look up CRS definitions referred to by the schema (if  needed for type conversion).
        """
        return sqlalchemy.Table(
            table_name,
            sqlalchemy.MetaData(),
            *[cls._column_def_for_column_schema(c, dataset) for c in schema],
            schema=db_schema,
        )

    @classmethod
    def _column_def_for_column_schema(cls, col, dataset=None):
        """
        Returns a sqlalchemy column definition with conversion-logic for reading or writing data with the given
        column-schema to or from the given dataset.

        col - a kart.schema.ColumnSchema
        dataset - this is used to look up CRS definitions referred to by the schema (if  needed for type conversion).
        """
        return sqlalchemy.Column(
            col.name,
            cls._type_def_for_column_schema(col, dataset),
            primary_key=col.pk_index is not None,
        )

    def _type_def_for_column_schema(cls, col, dataset=None):
        """
        Returns a ConverterType suitable for converting Kart values of type `col.datatype` to or from the equivalent
        SQL type for this type of database.
        Can simply return None if no type conversion is required - for instance the Kart value read for an "integer"
        should be int, and most DB-API drivers will return an int when an integral type is read, so no conversion needed.
        If a value read from the DB cannot be converted to the equivalent Kart type, it can be left as-is - this will
        be uncommittable, but the resulting error message gives the user a chance to find and fix the schema-violation.

        col - a kart.schema.ColumnSchema
        dataset - this is used to look up CRS definitions referred to by the schema (if  needed for type conversion).
        """
        raise NotImplementedError()

    # TODO - move other common functions - or at least declare their signatures - in BaseKartAdapter.


class ConverterType(UserDefinedType):
    """
    A User-defined-type that automatically converts values when reading and writing to the database.
    In SQLAlchemy, the most straight-forward way to create a type-converter is to define a user-defined-type that has
    extra logic when reading or writing - hence the name "converter-type".
    After each conversion step, the type of the resulting data is declared to be `self` - the user-defined-type - this
    is so that if there are more conversion steps at a different layer, they will still be run too.

    Subclasses should override some or all of the following:

    1. Called in Python layer before writing:
    def bind_processor(self, dialect):
        # Returns a converter function for pre-processing python values.

    2. Called in SQL layer during writing:
    def bind_expression(self, bindvalue):
        # Returns a SQL expression for writing the bindvalue to the database.

    At this point the data is at rest in the database. But, to continue the round-trip:

    3. Called in SQL layer during reading:
    def column_expression(self, column):
        # Returns a SQL expression for reading the column from the database.

    4. Called in Python layer after reading:
    def result_processor(self, dialect, coltype):
        # Returns a converter function for post-processing python values.
    """


def aliased_converter_type(cls):
    """
    A decorator that renames the functions in a ConverterType, so that the following methods definitions can
    be used instead of the sqlalchemy ones. This avoids overriding methods that are not needed since sqlalchemy
    tries to optimise by detecting which methods have been overridden and which are not.

    An @aliased_converter_type ConverterType should override some or all of the following:

    1. Called in Python layer before writing:
    def python_prewrite(self, value):
        # Pre-process value before writing to the database.

    2. Called in SQL layer during writing:
    def sql_write(self, bindvalue):
        # Returns a SQL expression for writing the bindvalue to the database.

    At this point the data is at rest in the database. But, to continue the round-trip:

    3. Called in SQL layer during reading:
    def sql_read(self, column):
        # Returns a SQL expression for reading the column from the database.

    4. Called in Python layer after reading:
    def python_postread(self, value):
        # Post-process value after reading from the database.
    """
    if hasattr(cls, "python_prewrite"):

        def bind_processor(self, dialect):
            return lambda v: self.python_prewrite(v)

        cls.bind_processor = bind_processor

    if hasattr(cls, "sql_write"):
        cls.bind_expression = cls.sql_write

    if hasattr(cls, "sql_read"):
        cls.column_expression = cls.sql_read

    if hasattr(cls, "python_postread"):

        def result_processor(self, dialect, coltype):
            return lambda v: self.python_postread(v)

        cls.result_processor = result_processor

    return cls