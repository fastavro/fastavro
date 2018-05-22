# cython: auto_cpdef=True

"""Python code for writing AVRO files"""

# This code is a modified version of the code at
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/ which is under
# Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)

import json

import datetime
import decimal
import time
import uuid
from binascii import crc32
from os import urandom, SEEK_SET
from struct import pack
from zlib import compress

from .validation import validate
from .const import (
    MCS_PER_HOUR, MCS_PER_MINUTE, MCS_PER_SECOND, MLS_PER_HOUR, MLS_PER_MINUTE,
    MLS_PER_SECOND, DAYS_SHIFT
)
from .six import utob, MemoryIO, long, iteritems, mk_bits
from .read import HEADER_SCHEMA, SYNC_SIZE, MAGIC
from .schema import (
    extract_named_schemas_into_repo, extract_record_type,
    extract_logical_type
)
from ._schema_common import SCHEMA_DEFS
from ._timezone import epoch

NoneType = type(None)


def write_null(fo, datum, schema=None):
    """null is written as zero bytes"""
    pass


def write_boolean(fo, datum, schema=None):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    fo.write(pack('B', 1 if datum else 0))


def prepare_timestamp_millis(data, schema):
    """Converts datetime.datetime object to int timestamp with milliseconds
    """
    if isinstance(data, datetime.datetime):
        if data.tzinfo is not None:
            delta = (data - epoch)
            return int(delta.total_seconds() * MLS_PER_SECOND)
        t = int(time.mktime(data.timetuple())) * MLS_PER_SECOND + int(
            data.microsecond / 1000)
        return t
    else:
        return data


def prepare_timestamp_micros(data, schema):
    """Converts datetime.datetime to int timestamp with microseconds"""
    if isinstance(data, datetime.datetime):
        if data.tzinfo is not None:
            delta = (data - epoch)
            return int(delta.total_seconds() * MCS_PER_SECOND)
        t = int(time.mktime(data.timetuple())) * MCS_PER_SECOND + \
            data.microsecond
        return t
    else:
        return data


def prepare_date(data, schema):
    """Converts datetime.date to int timestamp"""
    if isinstance(data, datetime.date):
        return data.toordinal() - DAYS_SHIFT
    else:
        return data


def prepare_uuid(data, schema):
    """Converts uuid.UUID to
    string formatted UUID xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    """
    if isinstance(data, uuid.UUID):
        return str(data)
    else:
        return data


def prepare_time_millis(data, schema):
    """Convert datetime.time to int timestamp with milliseconds"""
    if isinstance(data, datetime.time):
        return int(
            data.hour * MLS_PER_HOUR + data.minute * MLS_PER_MINUTE +
            data.second * MLS_PER_SECOND + int(data.microsecond / 1000))
    else:
        return data


def prepare_time_micros(data, schema):
    """Convert datetime.time to int timestamp with microseconds"""
    if isinstance(data, datetime.time):
        return long(data.hour * MCS_PER_HOUR + data.minute * MCS_PER_MINUTE +
                    data.second * MCS_PER_SECOND + data.microsecond)
    else:
        return data


def prepare_bytes_decimal(data, schema):
    """Convert decimal.Decimal to bytes"""
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

    tmp = MemoryIO()

    for index in range(bytes_req - 1, -1, -1):
        bits_to_write = packed_bits >> (8 * index)
        tmp.write(mk_bits(bits_to_write & 0xff))

    return tmp.getvalue()


def prepare_fixed_decimal(data, schema):
    """Converts decimal.Decimal to fixed length bytes array"""
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

    tmp = MemoryIO()

    if sign:
        unscaled_datum = (1 << bits_req) - unscaled_datum
        unscaled_datum = mask | unscaled_datum
        for index in range(size - 1, -1, -1):
            bits_to_write = unscaled_datum >> (8 * index)
            tmp.write(mk_bits(bits_to_write & 0xff))
    else:
        for i in range(offset_bits // 8):
            tmp.write(mk_bits(0))
        for index in range(bytes_req - 1, -1, -1):
            bits_to_write = unscaled_datum >> (8 * index)
            tmp.write(mk_bits(bits_to_write & 0xff))

    return tmp.getvalue()


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


def write_union(fo, datum, schema):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value. The value
    is then encoded per the indicated schema within the union."""

    if isinstance(datum, tuple):
        (name, datum) = datum
        for index, candidate in enumerate(schema):
            if extract_record_type(candidate) == 'record':
                schema_name = candidate['name']
            else:
                schema_name = candidate
            if name == schema_name:
                break
        else:
            msg = 'provided union type name %s not found in schema %s' \
              % (name, schema)
            raise ValueError(msg)
    else:
        pytype = type(datum)
        best_match_index = -1
        most_fields = -1
        for index, candidate in enumerate(schema):
            if validate(datum, candidate, raise_errors=False):
                if extract_record_type(candidate) == 'record':
                    fields = len(candidate['fields'])
                    if fields > most_fields:
                        best_match_index = index
                        most_fields = fields
                else:
                    best_match_index = index
                    break
        if best_match_index < 0:
            msg = '%r (type %s) do not match %s' % (datum, pytype, schema)
            raise ValueError(msg)
        index = best_match_index

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
        if name not in datum and 'default' not in field and \
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


def write_data(fo, datum, schema):
    """Write a datum of data to output stream.

    Paramaters
    ----------
    fo: file-like
        Output file
    datum: object
        Data to write
    schema: dict
        Schemda to use
    """

    record_type = extract_record_type(schema)
    logical_type = extract_logical_type(schema)

    fn = WRITERS[record_type]

    if logical_type:
        prepare = LOGICAL_WRITERS.get(logical_type)
        if prepare:
            datum = prepare(datum, schema)
    return fn(fo, datum, schema)


dump = write_data


def write_header(fo, metadata, sync_marker):
    header = {
        'magic': MAGIC,
        'meta': {key: utob(value) for key, value in iteritems(metadata)},
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


def acquaint_schema(schema):
    """Extract schema into WRITERS"""
    extract_named_schemas_into_repo(
        schema,
        WRITERS,
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
        self.block_count = 0

    def write(self, record):
        if self.validate_fn:
            self.validate_fn(record, self.schema)
        write_data(self.io, record, self.schema)
        self.block_count += 1
        if self.io.tell() >= self.sync_interval:
            self.dump()

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

    Parameters
    ----------
    fo: file-like
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
        then fastavro.validation.validate will be used. If it's a function, it
        should have the same signature as fastavro.writer.validate and raise an
        exeption on error.



    Example::

        from fastavro import writer

        schema = {
            'doc': 'A weather reading.',
            'name': 'Weather',
            'namespace': 'test',
            'type': 'record',
            'fields': [
                {'name': 'station', 'type': 'string'},
                {'name': 'time', 'type': 'long'},
                {'name': 'temp', 'type': 'int'},
            ],
        }

        records = [
            {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
            {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
            {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
            {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
        ]

        with open('weather.avro', 'wb') as out:
            writer(out, schema, records)
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

    Parameters
    ----------
    fo: file-like
        Output file
    schema: dict
        Schema
    record: dict
        Record to write



    Example::

        with open('file.avro', 'rb') as fp:
            fastavro.schemaless_writer(fp, schema, record)

    Note: The ``schemaless_writer`` can only write a single record.
    """
    acquaint_schema(schema)
    write_data(fo, record, schema)
