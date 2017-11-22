from _schema_public import UnknownType

try:
    from ._schema import acquaint_schema, load_schema
except ImportError:
    from ._schema_py import acquaint_schema, load_schema

