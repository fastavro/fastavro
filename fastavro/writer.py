# cython: auto_cpdef=True

'''Python code for writing AVRO files'''

# This code is a modified version of the code at
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/ which is under
# Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)

try:
    from ._six import utob, unicode, MemoryIO, long
    from ._reader import MASK, HEADER_SCHEMA, SYNC_SIZE, MAGIC
except ImportError:
    from .six import utob, unicode, MemoryIO, long
    from .reader import MASK, HEADER_SCHEMA, SYNC_SIZE, MAGIC

from binascii import crc32
from os import urandom, SEEK_SET
from struct import pack, unpack
import json

NoneType = type(None)


def write_null(fo, datum, schema=None):
    '''null is written as zero bytes'''
    pass


def write_boolean(fo, datum, schema=None):
    '''A boolean is written as a single byte whose value is either 0 (false) or
    1 (true).'''
    fo.write(chr(1) if datum else chr(0))


def write_int(fo, datum, schema=None):
    '''int and long values are written using variable-length, zig-zag coding.
    '''
    datum = (datum << 1) ^ (datum >> 63)
    while (datum & ~0x7F) != 0:
        fo.write(chr((datum & 0x7f) | 0x80))
        datum >>= 7
    fo.write(chr(datum))

write_long = write_int


def write_float(fo, datum, schema=None):
    '''A float is written as 4 bytes.  The float is converted into a 32-bit
    integer using a method equivalent to Java's floatToIntBits and then encoded
    in little-endian format.'''
    bits = unpack('!I', pack('!f', datum))[0]

    fo.write(chr((bits) & MASK))
    fo.write(chr((bits >> 8) & MASK))
    fo.write(chr((bits >> 16) & MASK))
    fo.write(chr((bits >> 24) & MASK))


def write_double(fo, datum, schema=None):
    '''A double is written as 8 bytes.  The double is converted into a 64-bit
    integer using a method equivalent to Java's doubleToLongBits and then
    encoded in little-endian format.  '''
    bits = unpack('!Q', pack('!d', datum))[0]

    fo.write(chr((bits) & MASK))
    fo.write(chr((bits >> 8) & MASK))
    fo.write(chr((bits >> 16) & MASK))
    fo.write(chr((bits >> 24) & MASK))
    fo.write(chr((bits >> 32) & MASK))
    fo.write(chr((bits >> 40) & MASK))
    fo.write(chr((bits >> 48) & MASK))
    fo.write(chr((bits >> 56) & MASK))


def write_bytes(fo, datum, schema=None):
    '''Bytes are encoded as a long followed by that many bytes of data.'''
    write_long(fo, len(datum))
    fmt = '{}s'.format(len(datum))
    fo.write(pack(fmt, datum))


def write_utf8(fo, datum, schema=None):
    '''A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data.'''
    datum = utob(datum)
    write_bytes(fo, datum)


def write_crc32(fo, bytes):
    '''A 4-byte, big-endian CRC32 checksum'''
    data = crc32(bytes) & 0xFFFFFFFF
    fo.write(pack('>I', data))


def write_fixed(fo, datum, schema=None):
    '''Fixed instances are encoded using the number of bytes declared in the
    schema.'''
    fo.write(datum)


def write_enum(fo, datum, schema):
    """An enum is encoded by a int, representing the zero-based position of
    the symbol in the schema."""
    index = schema['symbols'].index(datum)
    write_int(fo, index)


def write_array(fo, datum, schema):
    """Arrays are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many array
    items.  A block with count zero indicates the end of the array.  Each item
    is encoded per the array's item schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.  """

    if not datum:
        return

    dtype = schema['items']
    write_long(fo, len(datum))
    for item in datum:
        write_data(fo, item, dtype)
    write_long(fo, 0)


def write_map(fo, datum, schema):
    """Maps are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many key/value
    pairs.  A block with count zero indicates the end of the map.  Each item is
    encoded per the map's value schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block. The actual
    count in this case is the absolute value of the count written."""
    if not datum:
        return

    vtype = schema['values']
    write_long(fo, len(datum))
    for key, val in datum.iteritems():
        write_utf8(fo, key)
        write_data(fo, val, vtype)
    write_long(fo, 0)


typeconv = {
    'null': set([NoneType]),
    'boolean': set([bool]),
    'string': set([str, unicode]),
    'bytes': set([bytes]),
    'int': set([int, long]),
    'long': set([int, long]),
    'float': set([int, long, float]),
    'double': set([int, long, float]),
}


def write_union(fo, datum, schema):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value. The value
    is then encoded per the indicated schema within the union."""

    pytype = type(datum)
    for index, atype in enumerate(schema):
        if pytype in typeconv[atype]:
            break
    else:
        raise ValueError('{} (type {}) do not match {}'.format(
            datum, pytype, schema))

    # write data
    write_long(fo, index)
    write_data(fo, datum, schema[index])


def write_record(fo, datum, schema):
    """A record is encoded by encoding the values of its fields in the order
    that they are declared. In other words, a record is encoded as just the
    concatenation of the encodings of its fields.  Field values are encoded per
    their schema."""
    for field in schema['fields']:
        write_data(fo, datum[field['name']], field['type'])


WRITERS = {
    'null': write_null,
    'boolean': write_boolean,
    'string': write_utf8,
    'int': write_long,
    'long': write_long,
    'float': write_float,
    'double': write_double,
    'bytes': write_bytes,
    'fixed': write_fixed,
    'enum': write_enum,
    'array': write_array,
    'map': write_map,
    'union': write_union,
    'error_union': write_union,
    'record': write_record,
}


def write_data(fo, datum, schema):
    '''Write data to file object according to schema.'''
    st = type(schema)
    if st is dict:
        record_type = schema['type']
    elif st is list:
        record_type = 'union'
    else:
        record_type = schema

    writer = WRITERS[record_type]
    return writer(fo, datum, schema)


def write_header(fo, schema, sync_marker):
    header = {
        'magic': MAGIC,
        'meta': {
            'avro.codec': 'null',  # FIXME: Compression
            'avro.schema': utob(json.dumps(schema)),
        },
        'sync': sync_marker
    }
    write_data(fo, header, HEADER_SCHEMA)


def write(fo, schema, records):
    sync_marker = urandom(SYNC_SIZE)
    write_header(fo, schema, sync_marker)
    sync_interval = 1000 * SYNC_SIZE
    io = MemoryIO()

    nblocks = 0

    def dump():
        # FIXME: Compression
        write_long(fo, nblocks, schema)
        fo.write(io.getvalue())
        fo.write(sync_marker)
        io.truncate(0)
        io.seek(0, SEEK_SET)

    for record in records:
        write_data(io, record, schema)
        nblocks += 1
        if io.tell() >= sync_interval:
            dump()
            nblocks = 0

    if io.tell():
        dump()

    fo.flush()
