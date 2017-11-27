try:
    from . import _reader
except ImportError as e:
    from . import _reader_py as _reader

from ._reader_common import HEADER_SCHEMA, SYNC_SIZE, MAGIC
from ._reader_common import SchemaResolutionError

acquaint_schema = _reader.acquaint_schema
reader = iter_avro = _reader.iter_avro
schemaless_reader = _reader.schemaless_reader
read_data = _reader.read_data
is_avro = _reader.is_avro

READERS = _reader.READERS
LOGICAL_READERS = _reader.LOGICAL_READERS

__all__ = [
    'acquaint_schema', 'reader', 'schemaless_reader', 'read_data', 'is_avro',
    'HEADER_SCHEMA', 'SYNC_SIZE', 'MAGIC', 'SchemaResolutionError',
    'LOGICAL_READERS', 'READERS',
]
