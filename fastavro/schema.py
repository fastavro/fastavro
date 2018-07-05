try:
    from . import _schema
except ImportError:
    from . import _schema_py as _schema

from ._schema_common import UnknownType, SchemaParseException

# Private API
schema_name = _schema.schema_name
extract_record_type = _schema.extract_record_type
extract_logical_type = _schema.extract_logical_type

# Public API
load_schema = _schema.load_schema
parse_schema = _schema.parse_schema

__all__ = [
    'UnknownType', 'load_schema', 'SchemaParseException', 'parse_schema',
]
