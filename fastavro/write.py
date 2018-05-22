try:
    from . import _write
except ImportError as e:
    from . import _write_py as _write

SCHEMA_DEFS = _write.SCHEMA_DEFS
acquaint_schema = _write.acquaint_schema
dump = _write.dump
writer = _write.writer
Writer = _write.Writer
schemaless_writer = _write.schemaless_writer
write_data = _write.write_data

WRITERS = _write.WRITERS
LOGICAL_WRITERS = _write.LOGICAL_WRITERS

__all__ = [
    'SCHEMA_DEFS', 'acquaint_schema', 'writer', 'schemaless_writer',
    'write_data', 'WRITERS', 'LOGICAL_WRITERS',
]
