# cython: auto_cpdef=True

"""Python code for reading AVRO files"""

# This code is a modified version of the code at
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/ which is under
# Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)

from struct import unpack, error as StructError
from zlib import decompress
import datetime
from decimal import localcontext, Decimal
from uuid import UUID

try:
    import ujson as json
except ImportError:
    import json

from ._six import (
    btou, iteritems, is_str, str2ints, fstint
)
from ._schema import (
    extract_record_type, acquaint_schema, populate_schema_defs,
    extract_logical_type
)
from ._reader_common import (
    SchemaResolutionError, MAGIC, SYNC_SIZE, HEADER_SCHEMA
)
from .const import (
    MCS_PER_HOUR, MCS_PER_MINUTE, MCS_PER_SECOND, MLS_PER_HOUR, MLS_PER_MINUTE,
    MLS_PER_SECOND, DAYS_SHIFT
)

CYTHON_MODULE = 1  # Tests check this to confirm whether using the Cython code.

MASK = 0xFF
AVRO_TYPES = set([
    'boolean',
    'bytes',
    'double',
    'float',
    'int',
    'long',
    'null',
    'string',
    'fixed',
    'enum',
    'record',
    'error',
    'array',
    'map',
    'union',
    'request',
    'error_union'
])


class ReadError(Exception):
    pass


cpdef match_types(writer_type, reader_type):
    if isinstance(writer_type, list) or isinstance(reader_type, list):
        return True
    if writer_type == reader_type:
        return True
    # promotion cases
    elif writer_type == 'int' and reader_type in ['long', 'float', 'double']:
        return True
    elif writer_type == 'long' and reader_type in ['float', 'double']:
        return True
    elif writer_type == 'float' and reader_type == 'double':
        return True
    return False


cpdef match_schemas(w_schema, r_schema):
    error_msg = 'Schema mismatch: %s is not %s' % (w_schema, r_schema)
    if isinstance(w_schema, list):
        # If the writer is a union, checks will happen in read_union after the
        # correct schema is known
        return True
    elif isinstance(r_schema, list):
        # If the reader is a union, ensure one of the new schemas is the same
        # as the writer
        for schema in r_schema:
            if match_types(w_schema, schema):
                return True
        else:
            raise SchemaResolutionError(error_msg)
    else:
        # Check for dicts as primitive types are just strings
        if isinstance(w_schema, dict):
            w_type = w_schema['type']
        else:
            w_type = w_schema
        if isinstance(r_schema, dict):
            r_type = r_schema['type']
        else:
            r_type = r_schema

        if w_type == r_type == 'map':
            if match_types(w_schema['values'], r_schema['values']):
                return True
        elif w_type == r_type == 'array':
            if match_types(w_schema['items'], r_schema['items']):
                return True
        elif match_types(w_type, r_type):
            return True
        raise SchemaResolutionError(error_msg)


cpdef inline read_null(ReaderBase fo, writer_schema=None, reader_schema=None):
    """null is written as zero bytes."""
    return None


cpdef inline read_boolean(ReaderBase fo, writer_schema=None, reader_schema=None):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true).
    """
    cdef unsigned char ch_temp
    cdef bytes bytes_temp = fo.read(1)
    if len(bytes_temp) == 1:
        # technically 0x01 == true and 0x00 == false, but many languages will
        # cast anything other than 0 to True and only 0 to False
        ch_temp = bytes_temp[0]
        return ch_temp != 0
    else:
        raise ReadError


cpdef parse_timestamp(data, resolution):
    return datetime.datetime.fromtimestamp(data / resolution)


cpdef read_timestamp_millis(data, writer_schema=None, reader_schema=None):
    return parse_timestamp(data, float(MLS_PER_SECOND))


cpdef read_timestamp_micros(data, writer_schema=None, reader_schema=None):
    return parse_timestamp(data, float(MCS_PER_SECOND))


cpdef read_date(data, writer_schema=None, reader_schema=None):
    return datetime.date.fromordinal(data + DAYS_SHIFT)


cpdef read_uuid(data, writer_schema=None, reader_schema=None):
    return UUID(data)


cpdef read_time_millis(data, writer_schema=None, reader_schema=None):
    h = int(data / MLS_PER_HOUR)
    m = int(data / MLS_PER_MINUTE) % 60
    s = int(data / MLS_PER_SECOND) % 60
    mls = int(data % MLS_PER_SECOND) * 1000
    return datetime.time(h, m, s, mls)


cpdef read_time_micros(data, writer_schema=None, reader_schema=None):
    h = int(data / MCS_PER_HOUR)
    m = int(data / MCS_PER_MINUTE) % 60
    s = int(data / MCS_PER_SECOND) % 60
    mcs = data % MCS_PER_SECOND
    return datetime.time(h, m, s, mcs)


cpdef read_bytes_decimal(data, writer_schema=None, reader_schema=None):
    size = len(data)
    return _read_decimal(data, size, writer_schema)


cpdef read_fixed_decimal(data, writer_schema=None, reader_schema=None):
    size = writer_schema['size']
    return _read_decimal(data, size, writer_schema)


cpdef _read_decimal(data, size, writer_schema):
    """
    based on https://github.com/apache/avro/pull/82/
    """
    cdef int offset
    scale = writer_schema['scale']
    precision = writer_schema['precision']

    datum_byte = str2ints(data)

    unscaled_datum = 0
    msb = fstint(data)
    leftmost_bit = (msb >> 7) & 1
    if leftmost_bit == 1:
        modified_first_byte = datum_byte[0] ^ (1 << 7)
        datum_byte = [modified_first_byte] + datum_byte[1:]
        for offset in range(size):
            unscaled_datum <<= 8
            unscaled_datum += datum_byte[offset]
        unscaled_datum += pow(-2, (size * 8) - 1)
    else:
        for offset in range(size):
            unscaled_datum <<= 8
            unscaled_datum += (datum_byte[offset])

    with localcontext() as ctx:
        ctx.prec = precision
        scaled_datum = Decimal(unscaled_datum).scaleb(-scale)
    return scaled_datum


cpdef long long read_long(ReaderBase fo,
                          writer_schema=None,
                          reader_schema=None) except? -1:
    """int and long values are written using variable-length, zig-zag
    coding."""
    cdef unsigned long long b
    cdef long long n
    cdef int shift
    cdef bytes c = fo.read(1)

    # We do EOF checking only here, since most reader start here
    if not c:
        raise StopIteration

    b = <unsigned char>(c[0])
    n = b & 0x7F
    shift = 7

    while (b & 0x80) != 0:
        c = fo.read(1)
        b = <unsigned char>(c[0])
        n |= (b & 0x7F) << shift
        shift += 7

    return (n >> 1) ^ -(n & 1)


cdef union float_int:
    float f
    unsigned int n


cpdef read_float(ReaderBase fo, writer_schema=None, reader_schema=None):
    """A float is written as 4 bytes.

    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    cdef bytes data
    cdef unsigned char ch_data[4]
    cdef float_int fi
    data = fo.read(4)
    if len(data) == 4:
        ch_data[:4] = data
        fi.n = (ch_data[0] |
                (ch_data[1] << 8) |
                (ch_data[2] << 16) |
                (ch_data[3] << 24))
        return fi.f
    else:
        raise ReadError


cdef union double_long:
    double d
    unsigned long n


cpdef read_double(ReaderBase fo, writer_schema=None, reader_schema=None):
    """A double is written as 8 bytes.

    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    cdef bytes data
    cdef unsigned char ch_data[8]
    cdef double_long dl
    data = fo.read(8)
    if len(data) == 8:
        ch_data[:8] = data
        dl.n = (ch_data[0] |
                (<unsigned long>(ch_data[1]) << 8) |
                (<unsigned long>(ch_data[2]) << 16) |
                (<unsigned long>(ch_data[3]) << 24) |
                (<unsigned long>(ch_data[4]) << 32) |
                (<unsigned long>(ch_data[5]) << 40) |
                (<unsigned long>(ch_data[6]) << 48) |
                (<unsigned long>(ch_data[7]) << 56))
        return dl.d
    else:
        raise ReadError


cpdef read_bytes(ReaderBase fo, writer_schema=None, reader_schema=None):
    """Bytes are encoded as a long followed by that many bytes of data."""
    cdef long long size = read_long(fo)
    return fo.read(<long>size)


cpdef unicode read_utf8(ReaderBase fo, writer_schema=None, reader_schema=None):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data.
    """
    return btou(read_bytes(fo), 'utf-8')


cpdef read_fixed(ReaderBase fo, writer_schema, reader_schema=None):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
    return fo.read(writer_schema['size'])


cpdef read_enum(ReaderBase fo, writer_schema, reader_schema=None):
    """An enum is encoded by a int, representing the zero-based position of the
    symbol in the schema.
    """
    index = read_long(fo)
    symbol = writer_schema['symbols'][index]
    if reader_schema and symbol not in reader_schema['symbols']:
        symlist = reader_schema['symbols']
        msg = '%s not found in reader symbol list %s' % (symbol, symlist)
        raise SchemaResolutionError(msg)
    return symbol


cpdef read_array(ReaderBase fo, writer_schema, reader_schema=None):
    """Arrays are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many array
    items.  A block with count zero indicates the end of the array.  Each item
    is encoded per the array's item schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    """
    cdef list read_items
    cdef long long block_count
    cdef long i

    read_items = []

    block_count = read_long(fo)

    while block_count != 0:
        if block_count < 0:
            block_count = -block_count
            # Read block size, unused
            read_long(fo)

        if reader_schema:
            for i in range(block_count):
                read_items.append(_read_data(fo,
                                             writer_schema['items'],
                                             reader_schema['items']))
        else:
            for i in range(block_count):
                read_items.append(_read_data(fo, writer_schema['items']))
        block_count = read_long(fo)

    return read_items


cpdef read_map(ReaderBase fo, writer_schema, reader_schema=None):
    """Maps are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many key/value
    pairs.  A block with count zero indicates the end of the map.  Each item is
    encoded per the map's value schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    """
    cdef dict read_items
    cdef long long block_count
    cdef long i
    cdef unicode key

    read_items = {}
    block_count = read_long(fo)
    while block_count != 0:
        if block_count < 0:
            block_count = -block_count
            # Read block size, unused
            read_long(fo)

        if reader_schema:
            for i in range(block_count):
                key = read_utf8(fo)
                read_items[key] = _read_data(fo,
                                             writer_schema['values'],
                                             reader_schema['values'])
        else:
            for i in range(block_count):
                key = read_utf8(fo)
                read_items[key] = _read_data(fo, writer_schema['values'])
        block_count = read_long(fo)

    return read_items


cpdef read_union(ReaderBase fo, writer_schema, reader_schema=None):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value.

    The value is then encoded per the indicated schema within the union.
    """
    # schema resolution
    index = read_long(fo)
    if reader_schema:
        # Handle case where the reader schema is just a single type (not union)
        if not isinstance(reader_schema, list):
            if match_types(writer_schema[index], reader_schema):
                return _read_data(fo, writer_schema[index], reader_schema)
        else:
            for schema in reader_schema:
                if match_types(writer_schema[index], schema):
                    return _read_data(fo, writer_schema[index], schema)
        msg = 'schema mismatch: %s not found in %s' % \
            (writer_schema, reader_schema)
        raise SchemaResolutionError(msg)
    else:
        return _read_data(fo, writer_schema[index])


cpdef read_record(ReaderBase fo, writer_schema, reader_schema=None):
    """A record is encoded by encoding the values of its fields in the order
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
    """
    record = {}
    if reader_schema is None:
        for field in writer_schema['fields']:
            record[field['name']] = _read_data(fo, field['type'])
    else:
        readers_field_dict = {f['name']: f for f in reader_schema['fields']}
        for field in writer_schema['fields']:
            readers_field = readers_field_dict.get(field['name'])
            if readers_field:
                record[field['name']] = _read_data(fo,
                                                   field['type'],
                                                   readers_field['type'])
            else:
                # should implement skip
                _read_data(fo, field['type'], field['type'])

        # fill in default values
        if len(readers_field_dict) > len(record):
            writer_fields = [f['name'] for f in writer_schema['fields']]
            for field_name, field in iteritems(readers_field_dict):
                if field_name not in writer_fields:
                    default = field.get('default')
                    if 'default' in field:
                        record[field['name']] = default
                    else:
                        msg = 'No default value for %s' % field['name']
                        raise SchemaResolutionError(msg)

    return record


LOGICAL_READERS = {
    'long-timestamp-millis': read_timestamp_millis,
    'long-timestamp-micros': read_timestamp_micros,
    'int-date': read_date,
    'bytes-decimal': read_bytes_decimal,
    'fixed-decimal': read_fixed_decimal,
    'string-uuid': read_uuid,
    'int-time-millis': read_time_millis,
    'long-time-micros': read_time_micros,
}

READERS = {
    'null': read_null,
    'boolean': read_boolean,
    'string': read_utf8,
    'int': read_long,
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


cpdef read_data(fo, writer_schema, reader_schema=None):
    return _read_data(FileObjectReader(fo), writer_schema, reader_schema)


cpdef _read_data(ReaderBase fo, writer_schema, reader_schema=None):
    """Read data from file object according to schema."""

    record_type = extract_record_type(writer_schema)
    logical_type = extract_logical_type(writer_schema)

    if reader_schema and record_type in AVRO_TYPES:
        match_schemas(writer_schema, reader_schema)
    try:
        data = READERS[record_type](fo, writer_schema, reader_schema)
        if 'logicalType' in writer_schema:
            fn = LOGICAL_READERS[logical_type]
            return fn(data, writer_schema, reader_schema)

        return data
    except ReadError:
        raise EOFError('cannot read %s from %s' % (record_type, fo))


cpdef skip_sync(ReaderBase fo, sync_marker):
    """Skip an expected sync marker, complaining if it doesn't match"""
    if fo.read(SYNC_SIZE) != sync_marker:
        raise ValueError('expected sync marker not found')


cdef class ReaderBase(object):
    cpdef bytes read(self, int size):
        raise NotImplemented


cdef class MemoryReader(ReaderBase):
    cdef bytes data
    cdef unsigned long position

    def __init__(self, data):
        self.data = data
        self.position = 0

    cpdef bytes read(self, int size):
        cdef bytes result
        result = self.data[self.position: self.position + size]
        self.position += size
        return result


cdef class FileObjectReader(ReaderBase):
    cdef object fo

    def __init__(self, fo):
        self.fo = fo

    cpdef bytes read(self, int size):
        return self.fo.read(size)


cpdef MemoryReader null_read_block(fo):
    """Read block in "null" codec."""
    length = read_long(fo, None)
    data = fo.read(length)
    return MemoryReader(data)


cpdef MemoryReader deflate_read_block(fo):
    """Read block in "deflate" codec."""
    data = read_bytes(fo, None)
    # -15 is the log of the window size; negative indicates "raw" (no
    # zlib headers) decompression.  See zlib.h.
    return MemoryReader(decompress(data, -15))


BLOCK_READERS = {
    'null': null_read_block,
    'deflate': deflate_read_block
}

try:
    import snappy

    BLOCK_READERS['snappy'] = snappy_read_block
except ImportError:
    pass


cpdef MemoryReader snappy_read_block(fo):
    length = read_long(fo, None)
    data = fo.read(length - 4)
    fo.read(4)  # CRC
    return MemoryReader(snappy.decompress(data))


def _iter_avro(ReaderBase fo, header, codec, writer_schema, reader_schema):
    """Return iterator over avro records."""
    cdef ReaderBase block_fo
    cdef int i

    sync_marker = header['sync']
    # Value in schema is bytes

    read_block = BLOCK_READERS.get(codec)
    if not read_block:
        raise ValueError('Unrecognized codec: %r' % codec)

    block_count = 0
    while True:
        block_count = read_long(fo, None)
        block_fo = read_block(fo)

        for i in range(block_count):
            yield _read_data(block_fo, writer_schema, reader_schema)

        skip_sync(fo, sync_marker)


class iter_avro:
    """Iterator over avro file."""
    def __init__(self, fo, reader_schema=None):
        """Creates a new iterator

        Parameters
        ----------
        fo: file like
            Input stream
        reader_schema: dict, optional
            Reader schema

        Example
        -------
        >>> with open('some-file.avro', 'rb') as fo:
        >>>     avro = iter_avro(fo)
        >>>     schema = avro.schema
        >>>     for record in avro:
        >>>         process_record(record)
        """
        self.fo = fo
        try:
            fo_reader = FileObjectReader(fo)
            self._header = _read_data(fo_reader, HEADER_SCHEMA)
        except StopIteration:
            raise ValueError('cannot read header - is it an avro file?')

        # `meta` values are bytes. So, the actual decoding has to be external.
        self.metadata = {
            k: btou(v) for k, v in iteritems(self._header['meta'])
        }

        self.schema = self.writer_schema = \
            json.loads(self.metadata['avro.schema'])
        self.codec = self.metadata.get('avro.codec', 'null')
        self.reader_schema = reader_schema

        acquaint_schema(self.writer_schema, READERS)
        if reader_schema:
            populate_schema_defs(reader_schema)
        self._records = _iter_avro(fo_reader,
                                   self._header,
                                   self.codec,
                                   self.writer_schema,
                                   reader_schema)

    def __iter__(self):
        return self._records

    def next(self):
        return next(self._records)

    __next__ = next


cpdef schemaless_reader(fo, schema):
    """Reads a single record writen using the schemaless_writer

    Parameters
    ----------
    fo: file like
        Input stream
    schema: dict
        Reader schema
    """
    acquaint_schema(schema, READERS)
    return read_data(fo, schema)


cpdef is_avro(path_or_buffer):
    """Return True if path (or buffer) points to an Avro file.

    Parameters
    ----------
    path_or_buffer: path to file or file line object
        Path to file
    """
    if is_str(path_or_buffer):
        fp = open(path_or_buffer, 'rb')
        close = True
    else:
        fp = path_or_buffer
        close = False

    try:
        header = fp.read(len(MAGIC))
        return header == MAGIC
    finally:
        if close:
            fp.close()
