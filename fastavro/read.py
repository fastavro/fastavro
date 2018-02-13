try:
    from . import _read
except ImportError as e:
    from . import _read_py as _read

from ._read_common import (
    HEADER_SCHEMA, SYNC_SIZE, MAGIC, SchemaResolutionError
)

acquaint_schema = _read.acquaint_schema
reader = iter_avro = _read.reader
schemaless_reader = _read.schemaless_reader
read_data = _read.read_data
is_avro = _read.is_avro

READERS = _read.READERS
LOGICAL_READERS = _read.LOGICAL_READERS
BLOCK_READERS = _read.BLOCK_READERS

__all__ = [
    'acquaint_schema', 'reader', 'schemaless_reader', 'read_data', 'is_avro',
    'HEADER_SCHEMA', 'SYNC_SIZE', 'MAGIC', 'SchemaResolutionError',
    'LOGICAL_READERS', 'READERS', 'BLOCK_READERS',
]
