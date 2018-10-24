try:
    from . import _write
except ImportError:
    from . import _write_py as _write

# Private API

# Public API
writer = _write.writer
Writer = _write.Writer
schemaless_writer = _write.schemaless_writer
LOGICAL_WRITERS = _write.LOGICAL_WRITERS

__all__ = [
    'writer', 'Writer', 'schemaless_writer', 'LOGICAL_WRITERS',
]
