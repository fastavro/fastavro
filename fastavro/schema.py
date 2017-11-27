try:
    from . import _schema
except ImportError:
    from . import _schema_py as _schema

from ._schema_common import UnknownType

load_schema = _schema.load_schema
extract_record_type = _schema.extract_record_type
acquaint_schema = _schema.acquaint_schema
populate_schema_defs = _schema.populate_schema_defs
extract_logical_type = _schema.extract_logical_type
extract_named_schemas_into_repo = _schema.extract_named_schemas_into_repo

__all__ = [
    'UnknownType', 'load_schema', 'extract_record_type', 'acquaint_schema',
    'extract_logical_type', 'extract_named_schemas_into_repo',
]
