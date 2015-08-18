# cython: auto_cpdef=True

'''Python code for reading AVRO files'''

# This code is a modified version of the code at
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/ which is under
# Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)

import json
from os import SEEK_CUR
from struct import unpack
from zlib import decompress

try:
    from ._six import MemoryIO, xrange, btou, utob
    from ._schema import acquaint_schema, extract_record_type
except ImportError:
    from .six import MemoryIO, xrange, btou, utob
    from .schema import acquaint_schema, extract_record_type

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
MASK = 0xFF


def read_null(fo, schema):
    '''null is written as zero bytes.'''
    return None


def read_boolean(fo, schema):
    '''A boolean is written as a single byte whose value is either 0 (false) or
    1 (true).
    '''
    return ord(fo.read(1)) == 49


def read_int(fo, schema):
    return int(read_long(fo, schema))


def read_long(fo, schema):
    '''int and long values are written using variable-length, zig-zag
    coding.'''
    c = fo.read(1)

    # We do EOF checking only here, since most reader start here
    if not c:
        raise StopIteration

    b = ord(c)
    n = b & 0x7F
    shift = 7

    while (b & 0x80) != 0:
        b = ord(fo.read(1))
        n |= (b & 0x7F) << shift
        shift += 7

    return long((n >> 1) ^ -(n & 1))


def read_float(fo, schema):
    '''A float is written as 4 bytes.

    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    '''

    return unpack('<f', fo.read(4))[0]


def read_double(fo, schema):
    '''A double is written as 8 bytes.

    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    '''
    return unpack('<d', fo.read(8))[0]


def read_bytes(fo, schema):
    '''Bytes are encoded as a long followed by that many bytes of data.'''
    size = read_long(fo, schema)
    return fo.read(size)


def read_utf8(fo, schema):
    '''A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data.
    '''
    return btou(read_bytes(fo, schema), 'utf-8')


def read_fixed(fo, schema):
    '''Fixed instances are encoded using the number of bytes declared in the
    schema.'''
    return fo.read(schema['size'])


def read_enum(fo, schema):
    '''An enum is encoded by a int, representing the zero-based position of the
    symbol in the schema.
    '''
    index = read_long(fo, schema)
    return schema['symbols'][index]


def read_array(fo, schema):
    '''Arrays are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many array
    items.  A block with count zero indicates the end of the array.  Each item
    is encoded per the array's item schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    '''
    read_items = []

    block_count = read_long(fo, schema)

    while block_count != 0:
        if block_count < 0:
            block_count = -block_count
            # Read block size, unused
            read_long(fo, schema)

        for i in xrange(block_count):
            read_items.append(read_data(fo, schema['items']))
        block_count = read_long(fo, schema)

    return read_items


def read_map(fo, schema):
    '''Maps are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many key/value
    pairs.  A block with count zero indicates the end of the map.  Each item is
    encoded per the map's value schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    '''
    read_items = {}
    block_count = read_long(fo, schema)
    while block_count != 0:
        if block_count < 0:
            block_count = -block_count
            # Read block size, unused
            read_long(fo, schema)

        for i in xrange(block_count):
            key = read_utf8(fo, schema)
            read_items[key] = read_data(fo, schema['values'])
        block_count = read_long(fo, schema)

    return read_items


def read_union(fo, schema):
    '''A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value.

    The value is then encoded per the indicated schema within the union.
    '''
    # schema resolution
    index = read_long(fo, schema)
    return read_data(fo, schema[index])


def read_record(fo, schema):
    '''A record is encoded by encoding the values of its fields in the order
    that they are declared. In other words, a record is encoded as just the
    concatenation of the encodings of its fields.  Field values are encoded per
    their schema.

    Schema Resolution:
     * the ordering of fields may be different: fields are matched by name.
     * schemas for fields with the same name in both records are resolved
         recursively.
     * if the writer's record contains a field with a name not present in the
         reader's record, the writer's value for that field is ignored.
     * if the reader's record schema has a field that contains a default value,
         and writer's schema does not have a field with the same name, then the
         reader should use the default value from its field.
     * if the reader's record schema has a field with no default value, and
         writer's schema does not have a field with the same name, then the
         field's value is unset.
    '''
    record = {}
    for field in schema['fields']:
        record[field['name']] = read_data(fo, field['type'])

    return record

READERS = {
    'null': read_null,
    'boolean': read_boolean,
    'string': read_utf8,
    'int': read_int,
    'long': read_long,
    'float': read_float,
    'double': read_double,
    'bytes': read_bytes,
    'fixed': read_fixed,
    'enum': read_enum,
    'array': read_array,
    'map': read_map,
    'union': read_union,
    'error_union': read_union,
    'record': read_record,
    'error': read_record,
    'request': read_record,
}


def read_data(fo, schema):
    '''Read data from file object according to schema.'''

    return READERS[extract_record_type(schema)](fo, schema)


def skip_sync(fo, sync_marker):
    '''Skip sync marker, might raise StopIteration.'''
    mark = fo.read(SYNC_SIZE)

    if not mark:
        raise StopIteration

    if mark != sync_marker:
        fo.seek(-SYNC_SIZE, SEEK_CUR)


def null_read_block(fo):
    '''Read block in "null" codec.'''
    read_long(fo, None)
    return fo


def deflate_read_block(fo):
    '''Read block in "deflate" codec.'''
    data = read_bytes(fo, None)
    # -15 is the log of the window size; negative indicates "raw" (no
    # zlib headers) decompression.  See zlib.h.
    return MemoryIO(decompress(data, -15))

BLOCK_READERS = {
    'null': null_read_block,
    'deflate': deflate_read_block
}

try:
    import snappy

    def snappy_read_block(fo):
        length = read_long(fo, None)
        data = fo.read(length - 4)
        fo.read(4)  # CRC
        return MemoryIO(snappy.decompress(data))

    BLOCK_READERS['snappy'] = snappy_read_block
except ImportError:
    pass


def _iter_avro(fo, header, codec, schema):
    '''Return iterator over avro records.'''
    sync_marker = header['sync']
    # Value in schema is bytes

    read_block = BLOCK_READERS.get(codec)
    if not read_block:
        raise ValueError('Unrecognized codec: {0!r}'.format(codec))

    block_count = 0
    while True:
        skip_sync(fo, sync_marker)
        block_count = read_long(fo, None)
        block_fo = read_block(fo)

        for i in xrange(block_count):
            yield read_data(block_fo, schema)


class iter_avro:
    '''Custom iterator over avro file.

    Example:
        with open('some-file.avro', 'rb') as fo:
            avro = iter_avro(fo)
            schema = avro.schema

            for record in avro:
                process_record(record)
    '''
    def __init__(self, fo):
        self.fo = fo
        try:
            self._header = read_data(fo, HEADER_SCHEMA)
        except StopIteration:
            raise ValueError('cannot read header - is it an avro file?')

        # `meta` values are bytes. So, the actual decoding has to be external.
        self.schema = schema = \
            json.loads(btou(self._header['meta']['avro.schema']))
        self.codec = btou(self._header['meta'].get('avro.codec', b'null'))

        acquaint_schema(schema)
        self._records = _iter_avro(fo, self._header, self.codec, schema)

    def __iter__(self):
        return self._records

    def next(self):
        return next(self._records)
