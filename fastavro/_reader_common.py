try:
    from ._six import utob
except ImportError:
    from .six import utob

VERSION = 1
MAGIC = b'Obj' + utob(chr(VERSION))
SYNC_SIZE = 16
HEADER_SCHEMA = {
    'type': 'record',
    'name': 'org.apache.avro.file.Header',
    'fields': [
        {
            'name': 'magic',
            'type': {'type': 'fixed', 'name': 'magic', 'size': len(MAGIC)},
        },
        {
            'name': 'meta',
            'type': {'type': 'map', 'values': 'bytes'}
        },
        {
            'name': 'sync',
            'type': {'type': 'fixed', 'name': 'sync', 'size': SYNC_SIZE}
        },
    ]
}


class SchemaResolutionError(Exception):
    pass
