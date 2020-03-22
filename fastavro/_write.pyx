# cython: language_level=3str

"""Python code for writing AVRO files"""

# This code is a modified version of the code at
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/ which is under
# Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)

import json
from binascii import crc32
from os import urandom
import bz2
import zlib

from fastavro import const
from ._logical_writers import LOGICAL_WRITERS
from ._validation import validate
from ._six import utob, long, iteritems, appendable
from ._read import HEADER_SCHEMA, SYNC_SIZE, MAGIC, reader
from ._schema import extract_record_type, extract_logical_type, parse_schema
from ._schema_common import SCHEMA_DEFS

CYTHON_MODULE = 1  # Tests check this to confirm whether using the Cython code.

ctypedef int int32
ctypedef unsigned int uint32
ctypedef unsigned long long ulong64
ctypedef long long long64

cdef long64 MCS_PER_SECOND = const.MCS_PER_SECOND
cdef long64 MCS_PER_MINUTE = const.MCS_PER_MINUTE
cdef long64 MCS_PER_HOUR = const.MCS_PER_HOUR

cdef long64 MLS_PER_SECOND = const.MLS_PER_SECOND
cdef long64 MLS_PER_MINUTE = const.MLS_PER_MINUTE
cdef long64 MLS_PER_HOUR = const.MLS_PER_HOUR


cdef inline write_null(object fo, datum, schema=None):
    """null is written as zero bytes"""
    pass


cdef inline write_boolean(bytearray fo, bint datum, schema=None):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    cdef unsigned char ch_temp[1]
    ch_temp[0] = 1 if datum else 0
    fo += ch_temp[:1]


cdef inline write_int(bytearray fo, datum, schema=None):
    """int and long values are written using variable-length, zig-zag coding.
    """
    cdef ulong64 n
    cdef unsigned char ch_temp[1]
    n = (datum << 1) ^ (datum >> 63)
    while (n & ~0x7F) != 0:
        ch_temp[0] = (n & 0x7f) | 0x80
        fo += ch_temp[:1]
        n >>= 7
    ch_temp[0] = n
    fo += ch_temp[:1]


cdef inline write_long(bytearray fo, datum, schema=None):
    write_int(fo, datum, schema)


cdef union float_uint32:
    float f
    uint32 n


cdef inline write_float(bytearray fo, float datum, schema=None):
    """A float is written as 4 bytes.  The float is converted into a 32-bit
    integer using a method equivalent to Java's floatToIntBits and then encoded
    in little-endian format."""
    cdef float_uint32 fi
    cdef unsigned char ch_temp[4]

    fi.f = datum
    ch_temp[0] = fi.n & 0xff
    ch_temp[1] = (fi.n >> 8) & 0xff
    ch_temp[2] = (fi.n >> 16) & 0xff
    ch_temp[3] = (fi.n >> 24) & 0xff

    fo += ch_temp[:4]


cdef union double_ulong64:
    double d
    ulong64 n


cdef inline write_double(bytearray fo, double datum, schema=None):
    """A double is written as 8 bytes.  The double is converted into a 64-bit
    integer using a method equivalent to Java's doubleToLongBits and then
    encoded in little-endian format.  """
    cdef double_ulong64 fi
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


cdef inline write_bytes(bytearray fo, bytes datum, schema=None):
    """Bytes are encoded as a long followed by that many bytes of data."""
    write_long(fo, len(datum))
    fo += datum


cdef inline write_utf8(bytearray fo, datum, schema=None):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data."""
    write_bytes(fo, utob(datum))


cdef inline write_crc32(bytearray fo, bytes bytes):
    """A 4-byte, big-endian CRC32 checksum"""
    cdef unsigned char ch_temp[4]
    cdef uint32 data = crc32(bytes) & 0xFFFFFFFF

    ch_temp[0] = (data >> 24) & 0xff
    ch_temp[1] = (data >> 16) & 0xff
    ch_temp[2] = (data >> 8) & 0xff
    ch_temp[3] = data & 0xff
    fo += ch_temp[:4]


cdef inline write_fixed(bytearray fo, object datum, schema=None):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
    fo += datum


cdef inline write_enum(bytearray fo, datum, schema):
    """An enum is encoded by a int, representing the zero-based position of
    the symbol in the schema."""
    index = schema['symbols'].index(datum)
    write_int(fo, index)


cdef write_array(bytearray fo, list datum, schema):
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


cdef write_map(bytearray fo, object datum, dict schema):
    """Maps are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many key/value
    pairs.  A block with count zero indicates the end of the map.  Each item is
    encoded per the map's value schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block. The actual
    count in this case is the absolute value of the count written."""
    cdef dict d_datum
    try:
        d_datum = <dict?>(datum)
    except TypeError:
        # Slower, general-purpose code where datum is something besides a dict,
        # e.g. a collections.OrderedDict or collections.defaultdict.
        if len(datum) > 0:
            write_long(fo, len(datum))
            vtype = schema['values']
            for key, val in iteritems(datum):
                write_utf8(fo, key)
                write_data(fo, val, vtype)
        write_long(fo, 0)
    else:
        # Faster, special-purpose code where datum is a Python dict.
        if len(d_datum) > 0:
            write_long(fo, len(d_datum))
            vtype = schema['values']
            for key, val in iteritems(d_datum):
                write_utf8(fo, key)
                write_data(fo, val, vtype)
        write_long(fo, 0)


cdef write_union(bytearray fo, datum, schema):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value. The value
    is then encoded per the indicated schema within the union."""

    cdef int32 best_match_index
    cdef int32 most_fields
    cdef int32 index
    cdef int32 fields
    cdef str schema_name
    if isinstance(datum, tuple):
        (name, datum) = datum
        for index, candidate in enumerate(schema):
            if extract_record_type(candidate) == 'record':
                schema_name = candidate["name"]
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
                    candidate_fields = set(
                        f["name"] for f in candidate["fields"]
                    )
                    datum_fields = set(datum)
                    fields = len(candidate_fields.intersection(datum_fields))
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


cdef write_record(bytearray fo, object datum, dict schema):
    """A record is encoded by encoding the values of its fields in the order
    that they are declared. In other words, a record is encoded as just the
    concatenation of the encodings of its fields.  Field values are encoded per
    their schema."""
    cdef list fields
    cdef dict field
    cdef dict d_datum
    fields = schema['fields']
    try:
        d_datum = <dict?>(datum)
    except TypeError:
        # Slower, general-purpose code where datum is something besides a dict,
        # e.g. a collections.OrderedDict or collections.defaultdict.
        for field in fields:
            name = field['name']
            if name not in datum and 'default' not in field and \
                    'null' not in field['type']:
                raise ValueError('no value and no default for %s' % name)
            write_data(fo, datum.get(
                name, field.get('default')), field['type'])
    else:
        # Faster, special-purpose code where datum is a Python dict.
        for field in fields:
            name = field['name']
            if name not in d_datum and 'default' not in field and \
                    'null' not in field['type']:
                raise ValueError('no value and no default for %s' % name)
            write_data(fo, d_datum.get(
                name, field.get('default')), field['type'])


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
            prepare = LOGICAL_WRITERS.get(logical_type)
            if prepare:
                datum = prepare(datum, schema)

    record_type = extract_record_type(schema)
    if record_type == 'null':
        return write_null(fo, datum, schema)
    elif record_type == 'string':
        return write_utf8(fo, datum, schema)
    elif record_type == 'int' or record_type == 'long':
        return write_long(fo, datum, schema)
    elif record_type == 'float':
        return write_float(fo, datum, schema)
    elif record_type == 'double':
        return write_double(fo, datum, schema)
    elif record_type == 'boolean':
        return write_boolean(fo, datum, schema)
    elif record_type == 'bytes':
        return write_bytes(fo, datum, schema)
    elif record_type == 'fixed':
        return write_fixed(fo, datum, schema)
    elif record_type == 'enum':
        return write_enum(fo, datum, schema)
    elif record_type == 'array':
        return write_array(fo, datum, schema)
    elif record_type == 'map':
        return write_map(fo, datum, schema)
    elif record_type == 'union' or record_type == 'error_union':
        return write_union(fo, datum, schema)
    elif record_type == 'record' or record_type == 'error':
        return write_record(fo, datum, schema)
    else:
        return write_data(fo, datum, SCHEMA_DEFS[record_type])


cpdef write_header(bytearray fo, dict metadata, bytes sync_marker):
    header = {
        'magic': MAGIC,
        'meta': {key: utob(value) for key, value in iteritems(metadata)},
        'sync': sync_marker
    }
    write_data(fo, header, HEADER_SCHEMA)


cpdef null_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "null" codec."""
    cdef bytearray tmp = bytearray()
    write_long(tmp, len(block_bytes))
    fo.write(tmp)
    fo.write(block_bytes)


cpdef deflate_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "deflate" codec."""
    cdef bytearray tmp = bytearray()
    # The first two characters and last character are zlib
    # wrappers around deflate data.
    if compression_level is not None:
        data = zlib.compress(block_bytes, compression_level)[2:-1]
    else:
        data = zlib.compress(block_bytes)[2:-1]

    write_long(tmp, len(data))
    fo.write(tmp)
    fo.write(data)


cpdef bzip2_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "bzip2" codec."""
    cdef bytearray tmp = bytearray()
    data = bz2.compress(block_bytes)
    write_long(tmp, len(data))
    fo.write(tmp)
    fo.write(data)


BLOCK_WRITERS = {
    'null': null_write_block,
    'deflate': deflate_write_block,
    'bzip2': bzip2_write_block,
}


def _missing_dependency(codec, library):
    def missing(fo, block_bytes, compression_level):
        raise ValueError(
            "{} codec is supported but you ".format(codec)
            + "need to install {}".format(library)
        )
    return missing


try:
    import snappy
except ImportError:
    BLOCK_WRITERS['snappy'] = _missing_dependency("snappy", "python-snappy")
else:
    BLOCK_WRITERS['snappy'] = snappy_write_block


cpdef snappy_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "snappy" codec."""
    cdef bytearray tmp = bytearray()
    data = snappy.compress(block_bytes)

    write_long(tmp, len(data) + 4)  # for CRC
    fo.write(tmp)
    fo.write(data)
    tmp[:] = b''
    write_crc32(tmp, block_bytes)
    fo.write(tmp)


try:
    import zstandard as zstd
except ImportError:
    BLOCK_WRITERS["zstandard"] = _missing_dependency("zstandard", "zstandard")
else:
    BLOCK_WRITERS["zstandard"] = zstandard_write_block


cpdef zstandard_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "zstandard" codec."""
    cdef bytearray tmp = bytearray()
    data = zstd.ZstdCompressor().compress(block_bytes)
    write_long(tmp, len(data))
    fo.write(tmp)
    fo.write(data)


try:
    import lz4.block
except ImportError:
    BLOCK_WRITERS["lz4"] = _missing_dependency("lz4", "lz4")
else:
    BLOCK_WRITERS["lz4"] = lz4_write_block


cpdef lz4_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "lz4" codec."""
    cdef bytearray tmp = bytearray()
    data = lz4.block.compress(block_bytes)
    write_long(tmp, len(data))
    fo.write(tmp)
    fo.write(data)


try:
    import lzma
except ImportError:
    try:
        from backports import lzma
    except ImportError:
        BLOCK_WRITERS["xz"] = _missing_dependency("xz", "backports.lzma")
    else:
        BLOCK_WRITERS["xz"] = xz_write_block
else:
    BLOCK_WRITERS["xz"] = xz_write_block


cpdef xz_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "xz" codec."""
    cdef bytearray tmp = bytearray()
    data = lzma.compress(block_bytes)
    write_long(tmp, len(data))
    fo.write(tmp)
    fo.write(data)


cdef class MemoryIO(object):
    cdef bytearray value

    def __init__(self):
        self.value = bytearray()

    cpdef int32 tell(self):
        return len(self.value)

    cpdef bytes getvalue(self):
        return bytes(self.value)

    cpdef clear(self):
        self.value[:] = b''


cdef class Writer(object):
    cdef public object fo
    cdef public object schema
    cdef public object validate_fn
    cdef public object sync_marker
    cdef public MemoryIO io
    cdef public int32 block_count
    cdef public object metadata
    cdef public long64 sync_interval
    cdef public object block_writer
    cdef public object compression_level

    def __init__(self,
                 fo,
                 schema,
                 codec='null',
                 sync_interval=1000 * SYNC_SIZE,
                 metadata=None,
                 validator=None,
                 sync_marker=None,
                 compression_level=None):
        cdef bytearray tmp = bytearray()

        self.fo = fo
        self.schema = parse_schema(schema)
        self.validate_fn = validate if validator is True else validator
        self.io = MemoryIO()
        self.block_count = 0
        self.sync_interval = sync_interval
        self.compression_level = compression_level

        if appendable(self.fo):
            # Seed to the beginning to read the header
            self.fo.seek(0)
            avro_reader = reader(self.fo)
            header = avro_reader._header

            file_writer_schema = parse_schema(avro_reader.writer_schema)
            if self.schema != file_writer_schema:
                msg = "Provided schema {} does not match file writer_schema {}"
                raise ValueError(msg.format(self.schema, file_writer_schema))

            codec = avro_reader.metadata.get("avro.codec", "null")

            self.sync_marker = header["sync"]

            # Seek to the end of the file
            self.fo.seek(0, 2)

            self.block_writer = BLOCK_WRITERS[codec]
        else:
            self.sync_marker = sync_marker or urandom(SYNC_SIZE)

            self.metadata = metadata or {}
            self.metadata['avro.codec'] = codec

            if isinstance(schema, dict):
                schema = {
                    key: value
                    for key, value in iteritems(schema)
                    if key != "__fastavro_parsed"
                }

            self.metadata['avro.schema'] = json.dumps(schema)

            try:
                self.block_writer = BLOCK_WRITERS[codec]
            except KeyError:
                raise ValueError('unrecognized codec: %r' % codec)

            write_header(tmp, self.metadata, self.sync_marker)
            self.fo.write(tmp)

    def dump(self):
        cdef bytearray tmp = bytearray()
        write_long(tmp, self.block_count)
        self.fo.write(tmp)
        self.block_writer(self.fo, self.io.getvalue(), self.compression_level)
        self.fo.write(self.sync_marker)
        self.io.clear()
        self.block_count = 0

    def write(self, record):
        if self.validate_fn:
            self.validate_fn(record, self.schema)
        write_data(self.io.value, record, self.schema)
        self.block_count += 1
        if self.io.tell() >= self.sync_interval:
            self.dump()

    def write_block(self, block):
        # Clear existing block if there are any records pending
        if self.io.tell() or self.block_count > 0:
            self.dump()
        cdef bytearray tmp = bytearray()
        write_long(tmp, block.num_records)
        self.fo.write(tmp)
        self.block_writer(self.fo, block.bytes_.getvalue(), self.compression_level)
        self.fo.write(self.sync_marker)

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
           validator=None,
           sync_marker=None,
           codec_compression_level=None):
    # Sanity check that records is not a single dictionary (as that is a common
    # mistake and the exception that gets raised is not helpful)
    if isinstance(records, dict):
        raise ValueError('"records" argument should be an iterable, not dict')

    output = Writer(
        fo,
        schema,
        codec,
        sync_interval,
        metadata,
        validator,
        sync_marker,
        codec_compression_level,
    )

    for record in records:
        output.write(record)
    output.flush()


def schemaless_writer(fo, schema, record):
    cdef bytearray tmp = bytearray()
    schema = parse_schema(schema)
    write_data(tmp, record, schema)
    fo.write(tmp)
