try:
    from . import _writer
except ImportError as e:
    from . import _writer_py as _writer

from ._writer_common import SCHEMA_DEFS

acquaint_schema = _writer.acquaint_schema
writer = _writer.writer
schemaless_writer = _writer.schemaless_writer
write_data = _writer.write_data

WRITERS = _writer.WRITERS
LOGICAL_WRITERS = _writer.LOGICAL_WRITERS

__all__ = [
    'SCHEMA_DEFS', 'acquaint_schema', 'writer', 'schemaless_writer',
    'write_data', 'WRITERS', 'LOGICAL_WRITERS',
]
