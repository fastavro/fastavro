try:
    from . import _writer
except ImportError as e:
    from . import _writer_py as _writer

from ._writer_common import SCHEMA_DEFS

acquaint_schema = _writer.acquaint_schema
dump = _writer.dump
writer = _writer.writer
Writer = _writer.Writer
schemaless_writer = _writer.schemaless_writer
write_data = _writer.write_data

WRITERS = _writer.WRITERS
LOGICAL_WRITERS = _writer.LOGICAL_WRITERS

__all__ = [
    'SCHEMA_DEFS', 'acquaint_schema', 'writer', 'schemaless_writer',
    'write_data', 'WRITERS', 'LOGICAL_WRITERS',
]
