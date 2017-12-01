"""Python code for writing AVRO files"""

# This code is a modified version of the code at
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/ which is under
# Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)

from ._six import utob, long, is_str, iterkeys, itervalues, iteritems, mk_bits
from ._reader import HEADER_SCHEMA, SYNC_SIZE, MAGIC
from ._schema import (
    extract_named_schemas_into_repo, extract_record_type,
    extract_logical_type
)
from ._writer_common import SCHEMA_DEFS

from fastavro.const import (
    MCS_PER_HOUR, MCS_PER_MINUTE, MCS_PER_SECOND, MLS_PER_HOUR, MLS_PER_MINUTE,
    MLS_PER_SECOND, DAYS_SHIFT
)

try:
    import ujson as json
except ImportError:
    import json

import datetime
import decimal
import time
from binascii import crc32
from collections import Iterable, Mapping
from os import urandom, SEEK_SET
from zlib import compress

NoneType = type(None)

CYTHON_MODULE = 1  # Tests check this to confirm whether using the Cython code.


cpdef inline write_null(object fo, datum, schema=None):
    """null is written as zero bytes"""
    pass


cpdef inline write_boolean(bytearray fo, bint datum, schema=None):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    cdef unsigned char ch_temp[1]
    ch_temp[0] = 1 if datum else 0
    fo += ch_temp[:1]


cpdef prepare_timestamp_millis(object data, schema):
    if isinstance(data, datetime.datetime):
        t = int(time.mktime(data.timetuple())) * MLS_PER_SECOND + int(
            data.microsecond / 1000)
        return t
    else:
        return data


cpdef prepare_timestamp_micros(object data, schema):
    if isinstance(data, datetime.datetime):
        t = int(time.mktime(data.timetuple())) * MCS_PER_SECOND + \
            data.microsecond
        return t
    else:
        return data


cpdef prepare_date(object data, schema):
    if isinstance(data, datetime.date):
        return data.toordinal() - DAYS_SHIFT
    else:
        return data


cpdef prepare_uuid(object data, schema):
    return str(data)


cpdef prepare_time_millis(object data, schema):
    if isinstance(data, datetime.time):
        return int(
            data.hour * MLS_PER_HOUR + data.minute * MLS_PER_MINUTE +
            data.second * MLS_PER_SECOND + int(data.microsecond / 1000))
    else:
        return data


cpdef prepare_time_micros(object data, schema):
    if isinstance(data, datetime.time):
        return long(data.hour * MCS_PER_HOUR + data.minute * MCS_PER_MINUTE +
                    data.second * MCS_PER_SECOND + data.microsecond)
    else:
        return data


cpdef prepare_bytes_decimal(object data, schema):
    cdef bytearray tmp
    if not isinstance(data, decimal.Decimal):
        return data
    scale = schema['scale']

    # based on https://github.com/apache/avro/pull/82/

    sign, digits, exp = data.as_tuple()

    if -exp > scale:
        raise ValueError(
            'Scale provided in schema does not match the decimal')
    delta = exp + scale
    if delta > 0:
        digits = digits + (0,) * delta

    unscaled_datum = 0
    for digit in digits:
        unscaled_datum = (unscaled_datum * 10) + digit

    bits_req = unscaled_datum.bit_length() + 1

    if sign:
        unscaled_datum = (1 << bits_req) - unscaled_datum

    bytes_req = bits_req // 8
    padding_bits = ~((1 << bits_req) - 1) if sign else 0
    packed_bits = padding_bits | unscaled_datum

    bytes_req += 1 if (bytes_req << 3) < bits_req else 0

    tmp = bytearray()

    for index in range(bytes_req - 1, -1, -1):
        bits_to_write = packed_bits >> (8 * index)
        tmp += mk_bits(bits_to_write & 0xff)

    return bytes(tmp)


cpdef prepare_fixed_decimal(object data, schema):
    cdef bytearray tmp
    if not isinstance(data, decimal.Decimal):
        return data
    scale = schema['scale']
    size = schema['size']

    # based on https://github.com/apache/avro/pull/82/

    sign, digits, exp = data.as_tuple()

    if -exp > scale:
        raise ValueError(
            'Scale provided in schema does not match the decimal')
    delta = exp + scale
    if delta > 0:
        digits = digits + (0,) * delta

    unscaled_datum = 0
    for digit in digits:
        unscaled_datum = (unscaled_datum * 10) + digit

    bits_req = unscaled_datum.bit_length() + 1

    size_in_bits = size * 8
    offset_bits = size_in_bits - bits_req

    mask = 2 ** size_in_bits - 1
    bit = 1
    for i in range(bits_req):
        mask ^= bit
        bit <<= 1

    if bits_req < 8:
        bytes_req = 1
    else:
        bytes_req = bits_req // 8
        if bits_req % 8 != 0:
            bytes_req += 1

    tmp = bytearray()

    if sign:
        unscaled_datum = (1 << bits_req) - unscaled_datum
        unscaled_datum = mask | unscaled_datum
        for index in range(size - 1, -1, -1):
            bits_to_write = unscaled_datum >> (8 * index)
            tmp += mk_bits(bits_to_write & 0xff)
    else:
        for i in range(offset_bits // 8):
            tmp += mk_bits(0)
        for index in range(bytes_req - 1, -1, -1):
            bits_to_write = unscaled_datum >> (8 * index)
            tmp += mk_bits(bits_to_write & 0xff)

    return tmp


cpdef inline write_int(bytearray fo, datum, schema=None):
    """int and long values are written using variable-length, zig-zag coding.
    """
    cdef unsigned long long n
    cdef unsigned char ch_temp[1]
    n = (datum << 1) ^ (datum >> 63)
    while (n & ~0x7F) != 0:
        ch_temp[0] = (n & 0x7f) | 0x80
        fo += ch_temp[:1]
        n >>= 7
    ch_temp[0] = n
    fo += ch_temp[:1]


cpdef inline write_long(bytearray fo, datum, schema=None):
    write_int(fo, datum, schema)


cdef union float_int:
    float f
    unsigned int n


cpdef inline write_float(bytearray fo, float datum, schema=None):
    """A float is written as 4 bytes.  The float is converted into a 32-bit
    integer using a method equivalent to Java's floatToIntBits and then encoded
    in little-endian format."""
    cdef float_int fi
    cdef unsigned char ch_temp[4]

    fi.f = datum
    ch_temp[0] = fi.n & 0xff
    ch_temp[1] = (fi.n >> 8) & 0xff
    ch_temp[2] = (fi.n >> 16) & 0xff
    ch_temp[3] = (fi.n >> 24) & 0xff

    fo += ch_temp[:4]


cdef union double_long:
    double d
    unsigned long long n


cpdef inline write_double(bytearray fo, double datum, schema=None):
    """A double is written as 8 bytes.  The double is converted into a 64-bit
    integer using a method equivalent to Java's doubleToLongBits and then
    encoded in little-endian format.  """
    cdef double_long fi
    cdef unsigned char ch_temp[8]

    fi.d = datum
    ch_temp[0] = fi.n & 0xff
    ch_temp[1] = (fi.n >> 8) & 0xff
    ch_temp[2] = (fi.n >> 16) & 0xff
    ch_temp[3] = (fi.n >> 24) & 0xff
    ch_temp[4] = (fi.n >> 32) & 0xff
    ch_temp[5] = (fi.n >> 40) & 0xff
    ch_temp[6] = (fi.n >> 48) & 0xff
    ch_temp[7] = (fi.n >> 56) & 0xff

    fo += ch_temp[:8]


cpdef inline write_bytes(bytearray fo, bytes datum, schema=None):
    """Bytes are encoded as a long followed by that many bytes of data."""
    write_long(fo, len(datum))
    fo += datum


cpdef inline write_utf8(bytearray fo, datum, schema=None):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data."""
    write_bytes(fo, utob(datum))


cpdef inline write_crc32(bytearray fo, bytes bytes):
    """A 4-byte, big-endian CRC32 checksum"""
    cdef unsigned char ch_temp[4]
    cdef unsigned int data = crc32(bytes) & 0xFFFFFFFF

    ch_temp[0] = (data >> 24) & 0xff
    ch_temp[1] = (data >> 16) & 0xff
    ch_temp[2] = (data >> 8) & 0xff
    ch_temp[3] = data & 0xff
    fo += ch_temp[:4]


cpdef inline write_fixed(bytearray fo, object datum, schema=None):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
    fo += datum


cpdef inline write_enum(bytearray fo, datum, schema):
    """An enum is encoded by a int, representing the zero-based position of
    the symbol in the schema."""
    index = schema['symbols'].index(datum)
    write_int(fo, index)


cpdef write_array(bytearray fo, list datum, schema):
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


cpdef write_map(bytearray fo, dict datum, dict schema):
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


cpdef validate(object datum, schema):
    """Determine if a python datum is an instance of a schema."""
    cdef list l_schema
    record_type = extract_record_type(schema)

    if record_type == 'null':
        return datum is None

    if record_type == 'boolean':
        return isinstance(datum, bool)

    if record_type == 'string':
        return is_str(datum)

    if record_type == 'bytes':
        return isinstance(datum, (bytes, decimal.Decimal))

    if record_type == 'int':
        return (
            (isinstance(datum, (int, long,)) and
             INT_MIN_VALUE <= datum <= INT_MAX_VALUE) or
            isinstance(datum, (
                datetime.time, datetime.datetime, datetime.date))
        )

    if record_type == 'long':
        return (
            (isinstance(datum, (int, long,)) and
             LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE) or
            isinstance(datum, (
                datetime.time, datetime.datetime, datetime.date))
        )

    if record_type in ['float', 'double']:
        return isinstance(datum, (int, long, float))

    if record_type == 'fixed':
        return (
            (isinstance(datum, bytes) and len(datum) == schema['size']) or
            (isinstance(datum, decimal.Decimal))
        )

    if record_type == 'union':
        l_schema = schema
        if isinstance(datum, tuple):
            (name, datum) = datum
            for candidate in l_schema:
                if extract_record_type(candidate) == 'record':
                    if name == candidate["name"]:
                        return validate(datum, candidate)
            else:
                return False
        for s in l_schema:
            if validate(datum, s):
                return True
        return False

    # dict-y types from here on.
    if record_type == 'enum':
        return datum in schema['symbols']

    if record_type == 'array':
        if not isinstance(datum, Iterable):
            return False
        for d in datum:
            if not validate(d, schema['items']):
                return False
        return True

    if record_type == 'map':
        if not isinstance(datum, Mapping):
            return False
        for k in iterkeys(datum):
            if not is_str(k):
                return False
        for v in itervalues(datum):
            if not validate(v, schema['values']):
                return False
        return True

    if record_type in ('record', 'error', 'request',):
        if not isinstance(datum, Mapping):
            return False
        for f in schema['fields']:
            if not validate(datum.get(f['name'], f.get('default')), f['type']):
                return False
        return True

    if record_type in SCHEMA_DEFS:
        return validate(datum, SCHEMA_DEFS[record_type])

    raise ValueError('unkown record type - %s' % record_type)


cpdef write_union(bytearray fo, datum, schema):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value. The value
    is then encoded per the indicated schema within the union."""

    if isinstance(datum, tuple):
        (name, datum) = datum
        for index, candidate in enumerate(schema):
            if extract_record_type(candidate) == 'record':
                if name == candidate["name"]:
                    break
        else:
            msg = 'provided union type name %s not found in schema %s' \
                % (name, schema)
            raise ValueError(msg)
    else:
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


cpdef write_record(bytearray fo, dict datum, dict schema):
    """A record is encoded by encoding the values of its fields in the order
    that they are declared. In other words, a record is encoded as just the
    concatenation of the encodings of its fields.  Field values are encoded per
    their schema."""
    cdef list fields
    cdef dict field
    fields = schema['fields']
    for field in fields:
        name = field['name']
        if name not in datum and 'default' not in field and\
                'null' not in field['type']:
            raise ValueError('no value and no default for %s' % name)
        write_data(fo, datum.get(
            name, field.get('default')), field['type'])


LOGICAL_WRITERS = {
    'long-timestamp-millis': prepare_timestamp_millis,
    'long-timestamp-micros': prepare_timestamp_micros,
    'int-date': prepare_date,
    'bytes-decimal': prepare_bytes_decimal,
    'fixed-decimal': prepare_fixed_decimal,
    'string-uuid': prepare_uuid,
    'int-time-millis': prepare_time_millis,
    'long-time-micros': prepare_time_micros,

}

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


cpdef write_data(bytearray fo, datum, schema):
    """Write a datum of data to output stream.

    Paramaters
    ----------
    fo: file like
        Output file
    datum: object
        Data to write
    schema: dict
        Schema to use
    """
    cdef str logical_type = None
    if isinstance(schema, dict):
        logical_type = extract_logical_type(schema)
        if logical_type:
            prepare = LOGICAL_WRITERS[logical_type]
            datum = prepare(datum, schema)

    record_type = extract_record_type(schema)
    if record_type == 'string':
        return write_utf8(fo, datum, schema)
    elif record_type == 'int' or record_type == 'long':
        return write_long(fo, datum, schema)
    elif record_type == 'float':
        return write_float(fo, datum, schema)
    elif record_type == 'double':
        return write_double(fo, datum, schema)
    elif record_type == 'boolean':
        return write_boolean(fo, datum, schema)
    else:
        fn = WRITERS[record_type]
        return fn(fo, datum, schema)


cpdef write_header(bytearray fo, dict metadata, bytes sync_marker):
    header = {
        'magic': MAGIC,
        'meta': {key: utob(value) for key, value in iteritems(metadata)},
        'sync': sync_marker
    }
    write_data(fo, header, HEADER_SCHEMA)


cpdef null_write_block(object fo, bytes block_bytes):
    """Write block in "null" codec."""
    cdef bytearray tmp = bytearray()
    write_long(tmp, len(block_bytes))
    fo.write(tmp)
    fo.write(block_bytes)


cpdef deflate_write_block(object fo, bytes block_bytes):
    """Write block in "deflate" codec."""
    cdef bytearray tmp = bytearray()
    # The first two characters and last character are zlib
    # wrappers around deflate data.
    data = compress(block_bytes)[2:-1]

    write_long(tmp, len(data))
    fo.write(tmp)
    fo.write(data)


BLOCK_WRITERS = {
    'null': null_write_block,
    'deflate': deflate_write_block
}


try:
    import snappy

    BLOCK_WRITERS['snappy'] = snappy_write_block
except ImportError:
    snappy = None


cpdef snappy_write_block(object fo, bytes block_bytes):
    """Write block in "snappy" codec."""
    cdef bytearray tmp = bytearray()
    assert snappy is not None
    data = snappy.compress(block_bytes)

    write_long(tmp, len(data) + 4)  # for CRC
    fo.write(tmp)
    fo.write(data)
    tmp[:] = b''
    write_crc32(tmp, block_bytes)
    fo.write(tmp)


def acquaint_schema(schema, repo=None):
    """Extract schema into repo (default WRITERS)"""
    repo = WRITERS if repo is None else repo
    extract_named_schemas_into_repo(
        schema,
        repo,
        lambda schema: lambda bytearray fo, datum, _: write_data(fo, datum, schema),
    )
    extract_named_schemas_into_repo(
        schema,
        SCHEMA_DEFS,
        lambda schema: schema,
    )


cdef class Writer(object):
    cdef object fo
    cdef object schema
    cdef object validate_fn
    cdef object sync_marker
    cdef bytearray io
    cdef int block_count
    cdef object metadata
    cdef long sync_interval
    cdef object block_writer

    def __init__(self,
                 fo,
                 schema,
                 codec='null',
                 sync_interval=1000 * SYNC_SIZE,
                 metadata=None,
                 validator=None):
        cdef bytearray tmp = bytearray()
        self.fo = fo
        self.schema = schema
        self.validate_fn = validate if validator is True else validator
        self.sync_marker = bytes(urandom(SYNC_SIZE))
        self.io = bytearray()
        self.block_count = 0
        self.metadata = metadata or {}
        self.metadata['avro.codec'] = codec
        self.metadata['avro.schema'] = json.dumps(schema)
        self.sync_interval = sync_interval

        try:
            self.block_writer = BLOCK_WRITERS[codec]
        except KeyError:
            raise ValueError('unrecognized codec: %r' % codec)

        write_header(tmp, self.metadata, self.sync_marker)
        self.fo.write(tmp)
        acquaint_schema(self.schema)

    def dump(self):
        cdef bytearray tmp = bytearray()
        write_long(tmp, self.block_count)
        self.fo.write(tmp)
        self.block_writer(self.fo, bytes(self.io))
        self.fo.write(self.sync_marker)
        self.io[:] = b''
        self.block_count = 0

    def write(self, record):
        if self.validate_fn:
            self.validate_fn(record, self.schema)
        write_data(self.io, record, self.schema)
        self.block_count += 1
        if len(self.io) >= self.sync_interval:
            self.dump()

    def flush(self):
        if len(self.io) or self.block_count > 0:
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
    cdef bytearray tmp = bytearray()
    acquaint_schema(schema)
    write_data(tmp, record, schema)
    fo.write(tmp)
