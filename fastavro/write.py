try:
    from . import _write
except ImportError as e:
    from . import _write_py as _write

# Private API
SCHEMA_DEFS = _write.SCHEMA_DEFS
acquaint_schema = _write.acquaint_schema
WRITERS = _write.WRITERS

# Public API
writer = _write.writer
Writer = _write.Writer
schemaless_writer = _write.schemaless_writer
LOGICAL_WRITERS = _write.LOGICAL_WRITERS

__all__ = [
    'writer', 'Writer', 'schemaless_writer', 'LOGICAL_WRITERS',
]
