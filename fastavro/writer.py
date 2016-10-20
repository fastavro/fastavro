# cython: auto_cpdef=True

"""Python code for writing AVRO files"""

# This code is a modified version of the code at
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/ which is under
# Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)

try:
    from fastavro._six import utob, MemoryIO, long, is_str, iteritems
    from fastavro._reader import HEADER_SCHEMA, SYNC_SIZE, MAGIC
    from fastavro._schema import extract_named_schemas_into_repo,\
        extract_record_type
except ImportError:
    from fastavro.six import utob, MemoryIO, long, is_str, iteritems
    from fastavro.reader import HEADER_SCHEMA, SYNC_SIZE, MAGIC
    from fastavro.schema import extract_named_schemas_into_repo,\
        extract_record_type

try:
    import simplejson as json
except ImportError:
    import json

from binascii import crc32
from collections import Iterable, Mapping
from os import urandom, SEEK_SET
from struct import pack
from zlib import compress

NoneType = type(None)


def write_null(fo, datum, schema=None):
    """null is written as zero bytes"""
    pass


def write_boolean(fo, datum, schema=None):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    fo.write(pack('B', 1 if datum else 0))


def write_int(fo, datum, schema=None):
    """int and long values are written using variable-length, zig-zag coding.
    """
    datum = (datum << 1) ^ (datum >> 63)
    while (datum & ~0x7F) != 0:
        fo.write(pack('B', (datum & 0x7f) | 0x80))
        datum >>= 7
    fo.write(pack('B', datum))

write_long = write_int


def write_float(fo, datum, schema=None):
    """A float is written as 4 bytes.  The float is converted into a 32-bit
    integer using a method equivalent to Java's floatToIntBits and then encoded
    in little-endian format."""
    fo.write(pack('<f', datum))


def write_double(fo, datum, schema=None):
    """A double is written as 8 bytes.  The double is converted into a 64-bit
    integer using a method equivalent to Java's doubleToLongBits and then
    encoded in little-endian format.  """
    fo.write(pack('<d', datum))


def write_bytes(fo, datum, schema=None):
    """Bytes are encoded as a long followed by that many bytes of data."""
    write_long(fo, len(datum))
    fo.write(datum)


def write_utf8(fo, datum, schema=None):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data."""
    write_bytes(fo, utob(datum))


def write_crc32(fo, bytes):
    """A 4-byte, big-endian CRC32 checksum"""
    data = crc32(bytes) & 0xFFFFFFFF
    fo.write(pack('>I', data))


def write_fixed(fo, datum, schema=None):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
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

    if len(datum) > 0:
        write_long(fo, len(datum))
        dtype = schema['items']
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
    if len(datum) > 0:
        write_long(fo, len(datum))
        vtype = schema['values']
        for key, val in iteritems(datum):
            write_utf8(fo, key)
            write_data(fo, val, vtype)
    write_long(fo, 0)


INT_MIN_VALUE = -(1 << 31)
INT_MAX_VALUE = (1 << 31) - 1
LONG_MIN_VALUE = -(1 << 63)
LONG_MAX_VALUE = (1 << 63) - 1


def validate(datum, schema):
    """Determine if a python datum is an instance of a schema."""

    record_type = extract_record_type(schema)

    if record_type == 'null':
        return datum is None

    if record_type == 'boolean':
        return isinstance(datum, bool)

    if record_type == 'string':
        return is_str(datum)

    if record_type == 'bytes':
        return isinstance(datum, bytes)

    if record_type == 'int':
        return (
            isinstance(datum, (int, long,)) and
            INT_MIN_VALUE <= datum <= INT_MAX_VALUE
        )

    if record_type == 'long':
        return (
            isinstance(datum, (int, long,)) and
            LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE
        )

    if record_type in ['float', 'double']:
        return isinstance(datum, (int, long, float))

    if record_type == 'fixed':
        return isinstance(datum, bytes) and len(datum) == schema['size']

    if record_type == 'union':
        return any(validate(datum, s) for s in schema)

    # dict-y types from here on.
    if record_type == 'enum':
        return datum in schema['symbols']

    if record_type == 'array':
        return (
            isinstance(datum, Iterable) and
            all(validate(d, schema['items']) for d in datum)
        )

    if record_type == 'map':
        return (
            isinstance(datum, Mapping) and
            all(is_str(k) for k in datum.keys()) and
            all(validate(v, schema['values']) for v in datum.values())
        )

    if record_type in ('record', 'error', 'request',):
        return (
            isinstance(datum, Mapping) and
            all(
                validate(datum.get(f['name'], f.get('default')), f['type'])
                for f in schema['fields']
            )
        )

    if record_type in SCHEMA_DEFS:
        return validate(datum, SCHEMA_DEFS[record_type])

    raise ValueError('unkown record type - %s' % record_type)


def write_union(fo, datum, schema):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value. The value
    is then encoded per the indicated schema within the union."""

    pytype = type(datum)
    for index, candidate in enumerate(schema):
        if validate(datum, candidate):
            break
    else:
        msg = '%r (type %s) do not match %s' % (datum, pytype, schema)
        raise ValueError(msg)

    # write data
    write_long(fo, index)
    write_data(fo, datum, schema[index])


def write_record(fo, datum, schema):
    """A record is encoded by encoding the values of its fields in the order
    that they are declared. In other words, a record is encoded as just the
    concatenation of the encodings of its fields.  Field values are encoded per
    their schema."""
    for field in schema['fields']:
        name = field['name']
        if name not in datum and 'default' not in field:
            raise ValueError('no value and no default for %s' % name)
        write_data(fo, datum.get(name, field.get('default')), field['type'])


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
    'error': write_record,
}

_base_types = [
    'boolean',
    'bytes',
    'double',
    'float',
    'int',
    'long',
    'null',
    'string',
]

SCHEMA_DEFS = dict((typ, typ) for typ in _base_types)


def write_data(fo, datum, schema):
    """Write a datum of data to output stream.

    Paramaters
    ----------
    fo: file like
        Output file
    datum: object
        Data to write
    schema: dict
        Schemda to use
    """
    return WRITERS[extract_record_type(schema)](fo, datum, schema)


def write_header(fo, metadata, sync_marker):
    header = {
        'magic': MAGIC,
        'meta': dict([(key, utob(value)) for key, value in
                      iteritems(metadata)]),
        'sync': sync_marker
    }
    write_data(fo, header, HEADER_SCHEMA)


def null_write_block(fo, block_bytes):
    """Write block in "null" codec."""
    write_long(fo, len(block_bytes))
    fo.write(block_bytes)


def deflate_write_block(fo, block_bytes):
    """Write block in "deflate" codec."""
    # The first two characters and last character are zlib
    # wrappers around deflate data.
    data = compress(block_bytes)[2:-1]

    write_long(fo, len(data))
    fo.write(data)


BLOCK_WRITERS = {
    'null': null_write_block,
    'deflate': deflate_write_block
}


try:
    import snappy

    def snappy_write_block(fo, block_bytes):
        """Write block in "snappy" codec."""
        data = snappy.compress(block_bytes)

        write_long(fo, len(data) + 4)  # for CRC
        fo.write(data)
        write_crc32(fo, block_bytes)

    BLOCK_WRITERS['snappy'] = snappy_write_block
except ImportError:
    pass


def acquaint_schema(schema, repo=None):
    """Extract schema into repo (default WRITERS)"""
    repo = WRITERS if repo is None else repo
    extract_named_schemas_into_repo(
        schema,
        repo,
        lambda schema: lambda fo, datum, _: write_data(fo, datum, schema),
    )
    extract_named_schemas_into_repo(
        schema,
        SCHEMA_DEFS,
        lambda schema: schema,
    )


class Writer(object):
    def __init__(self,
                 fo,
                 schema,
                 codec='null',
                 sync_interval=1000 * SYNC_SIZE,
                 metadata=None,
                 validator=None):
        self.fo = fo
        self.schema = schema
        self.validate_fn = validate if validator is True else validator
        self.sync_marker = urandom(SYNC_SIZE)
        self.io = MemoryIO()
        self.block_count = 0
        self.metadata = metadata or {}
        self.metadata['avro.codec'] = codec
        self.metadata['avro.schema'] = json.dumps(schema)
        self.sync_interval = sync_interval

        try:
            self.block_writer = BLOCK_WRITERS[codec]
        except KeyError:
            raise ValueError('unrecognized codec: %r' % codec)

        write_header(self.fo, self.metadata, self.sync_marker)
        acquaint_schema(self.schema)

    def dump(self):
        write_long(self.fo, self.block_count)
        self.block_writer(self.fo, self.io.getvalue())
        self.fo.write(self.sync_marker)
        self.io.truncate(0)
        self.io.seek(0, SEEK_SET)

    def write(self, record):
        if self.validate_fn:
            self.validate_fn(record, self.schema)
        write_data(self.io, record, self.schema)
        self.block_count += 1
        if self.io.tell() >= self.sync_interval:
            self.dump()
            self.block_count = 0

    def flush(self):
        if self.io.tell() or self.block_count > 0:
            self.dump()
        self.fo.flush()


def writer(fo,
           schema,
           records,
           codec='null',
           sync_interval=1000 * SYNC_SIZE,
           metadata=None,
           validator=None):
    """Write records to fo (stream) according to schema

    Paramaters
    ----------
    fo: file like
        Output stream
    records: iterable
        Records to write
    codec: string, optional
        Compression codec, can be 'null', 'deflate' or 'snappy' (if installed)
    sync_interval: int, optional
        Size of sync interval
    metadata: dict, optional
        Header metadata
    validator: None, True or a function
        Validator function. If None (the default) - no validation. If True then
        then fastavro.writer.validate will be used. If it's a function, it
        should have the same signature as fastavro.writer.validate and raise an
        exeption on error.



    Example
    -------

    >>> from fastavro import writer

    >>> schema = {
    >>>     'doc': 'A weather reading.',
    >>>     'name': 'Weather',
    >>>     'namespace': 'test',
    >>>     'type': 'record',
    >>>     'fields': [
    >>>         {'name': 'station', 'type': 'string'},
    >>>         {'name': 'time', 'type': 'long'},
    >>>         {'name': 'temp', 'type': 'int'},
    >>>     ],
    >>> }

    >>> records = [
    >>>     {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
    >>>     {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
    >>>     {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
    >>>     {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
    >>> ]

    >>> with open('weather.avro', 'wb') as out:
    >>>     writer(out, schema, records)
    """
    output = Writer(
        fo,
        schema,
        codec,
        sync_interval,
        metadata,
        validator,
    )

    for record in records:
        output.write(record)
    output.flush()


def schemaless_writer(fo, schema, record):
    """Write a single record without the schema or header information

    Paramaters
    ----------
    fo: file like
        Output file
    schema: dict
        Schema
    record: dict
        Record to write

    """
    acquaint_schema(schema)
    write_data(fo, record, schema)
