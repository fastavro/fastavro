# cython: auto_cpdef=True

"""Python code for writing AVRO files"""

# This code is a modified version of the code at
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/ which is under
# Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)

import json
from io import BytesIO
from os import urandom, SEEK_SET
import bz2
import lzma
import zlib

from .io.binary_encoder import BinaryEncoder
from .io.json_encoder import AvroJSONEncoder
from .validation import _validate
from .read import HEADER_SCHEMA, SYNC_SIZE, MAGIC, reader
from .logical_writers import LOGICAL_WRITERS
from .schema import extract_record_type, extract_logical_type, parse_schema
from ._write_common import _is_appendable


def write_null(encoder, datum, schema, named_schemas, fname):
    """null is written as zero bytes"""
    encoder.write_null()


def write_boolean(encoder, datum, schema, named_schemas, fname):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    encoder.write_boolean(datum)


def write_int(encoder, datum, schema, named_schemas, fname):
    """int and long values are written using variable-length, zig-zag coding."""
    encoder.write_int(datum)


def write_long(encoder, datum, schema, named_schemas, fname):
    """int and long values are written using variable-length, zig-zag coding."""
    encoder.write_long(datum)


def write_float(encoder, datum, schema, named_schemas, fname):
    """A float is written as 4 bytes.  The float is converted into a 32-bit
    integer using a method equivalent to Java's floatToIntBits and then encoded
    in little-endian format."""
    encoder.write_float(datum)


def write_double(encoder, datum, schema, named_schemas, fname):
    """A double is written as 8 bytes.  The double is converted into a 64-bit
    integer using a method equivalent to Java's doubleToLongBits and then
    encoded in little-endian format."""
    encoder.write_double(datum)


def write_bytes(encoder, datum, schema, named_schemas, fname):
    """Bytes are encoded as a long followed by that many bytes of data."""
    encoder.write_bytes(datum)


def write_utf8(encoder, datum, schema, named_schemas, fname):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data."""
    encoder.write_utf8(datum)


def write_crc32(encoder, datum):
    """A 4-byte, big-endian CRC32 checksum"""
    encoder.write_crc32(datum)


def write_fixed(encoder, datum, schema, named_schemas, fname):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
    encoder.write_fixed(datum)


def write_enum(encoder, datum, schema, named_schemas, fname):
    """An enum is encoded by a int, representing the zero-based position of
    the symbol in the schema."""
    index = schema["symbols"].index(datum)
    encoder.write_enum(index)


def write_array(encoder, datum, schema, named_schemas, fname):
    """Arrays are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many array
    items.  A block with count zero indicates the end of the array.  Each item
    is encoded per the array's item schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written."""
    encoder.write_array_start()
    if len(datum) > 0:
        encoder.write_item_count(len(datum))
        dtype = schema["items"]
        for item in datum:
            write_data(encoder, item, dtype, named_schemas, fname)
            encoder.end_item()
    encoder.write_array_end()


def write_map(encoder, datum, schema, named_schemas, fname):
    """Maps are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many key/value
    pairs.  A block with count zero indicates the end of the map.  Each item is
    encoded per the map's value schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block. The actual
    count in this case is the absolute value of the count written."""
    encoder.write_map_start()
    if len(datum) > 0:
        encoder.write_item_count(len(datum))
        vtype = schema["values"]
        for key, val in datum.items():
            encoder.write_utf8(key)
            write_data(encoder, val, vtype, named_schemas, fname)
    encoder.write_map_end()


def write_union(encoder, datum, schema, named_schemas, fname):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value. The value
    is then encoded per the indicated schema within the union."""

    best_match_index = -1
    if isinstance(datum, tuple):
        (name, datum) = datum
        for index, candidate in enumerate(schema):
            if extract_record_type(candidate) == "record":
                schema_name = candidate["name"]
            else:
                schema_name = candidate
            if name == schema_name:
                best_match_index = index
                break

        if best_match_index == -1:
            field = f"on field {fname}" if fname else ""
            msg = (
                f"provided union type name {name} not found in schema "
                + f"{schema} {field}"
            )
            raise ValueError(msg)
        index = best_match_index
    else:
        pytype = type(datum)
        most_fields = -1
        for index, candidate in enumerate(schema):
            if _validate(datum, candidate, named_schemas, raise_errors=False):
                if extract_record_type(candidate) == "record":
                    candidate_fields = set(f["name"] for f in candidate["fields"])
                    datum_fields = set(datum)
                    fields = len(candidate_fields.intersection(datum_fields))
                    if fields > most_fields:
                        best_match_index = index
                        most_fields = fields
                else:
                    best_match_index = index
                    break
        if best_match_index == -1:
            field = f"on field {fname}" if fname else ""
            raise ValueError(
                f"{repr(datum)} (type {pytype}) do not match {schema} {field}"
            )
        index = best_match_index

    # write data
    # TODO: There should be a way to give just the index
    encoder.write_index(index, schema[index])
    write_data(encoder, datum, schema[index], named_schemas, fname)


def write_record(encoder, datum, schema, named_schemas, fname):
    """A record is encoded by encoding the values of its fields in the order
    that they are declared. In other words, a record is encoded as just the
    concatenation of the encodings of its fields.  Field values are encoded per
    their schema."""
    for field in schema["fields"]:
        name = field["name"]
        if name not in datum and "default" not in field and "null" not in field["type"]:
            raise ValueError(f"no value and no default for {name}")
        write_data(
            encoder,
            datum.get(name, field.get("default")),
            field["type"],
            named_schemas,
            name,
        )


WRITERS = {
    "null": write_null,
    "boolean": write_boolean,
    "string": write_utf8,
    "int": write_int,
    "long": write_long,
    "float": write_float,
    "double": write_double,
    "bytes": write_bytes,
    "fixed": write_fixed,
    "enum": write_enum,
    "array": write_array,
    "map": write_map,
    "union": write_union,
    "error_union": write_union,
    "record": write_record,
    "error": write_record,
}


def write_data(encoder, datum, schema, named_schemas, fname):
    """Write a datum of data to output stream.

    Paramaters
    ----------
    encoder: encoder
        Type of encoder (e.g. binary or json)
    datum: object
        Data to write
    schema: dict
        Schemda to use
    named_schemas: dict
        Mapping of fullname to schema definition
    """

    record_type = extract_record_type(schema)
    logical_type = extract_logical_type(schema)

    fn = WRITERS.get(record_type)
    if fn:
        if logical_type:
            prepare = LOGICAL_WRITERS.get(logical_type)
            if prepare:
                datum = prepare(datum, schema)
        try:
            return fn(encoder, datum, schema, named_schemas, fname)
        except TypeError as ex:
            if fname:
                raise TypeError(f"{ex} on field {fname}")
            raise
    else:
        return write_data(encoder, datum, named_schemas[record_type], named_schemas, "")


def write_header(encoder, metadata, sync_marker):
    header = {
        "magic": MAGIC,
        "meta": {key: value.encode() for key, value in metadata.items()},
        "sync": sync_marker,
    }
    write_data(encoder, header, HEADER_SCHEMA, {}, "")


def null_write_block(encoder, block_bytes, compression_level):
    """Write block in "null" codec."""
    encoder.write_long(len(block_bytes))
    encoder._fo.write(block_bytes)


def deflate_write_block(encoder, block_bytes, compression_level):
    """Write block in "deflate" codec."""
    # The first two characters and last character are zlib
    # wrappers around deflate data.
    if compression_level is not None:
        data = zlib.compress(block_bytes, compression_level)[2:-1]
    else:
        data = zlib.compress(block_bytes)[2:-1]
    encoder.write_long(len(data))
    encoder._fo.write(data)


def bzip2_write_block(encoder, block_bytes, compression_level):
    """Write block in "bzip2" codec."""
    data = bz2.compress(block_bytes)
    encoder.write_long(len(data))
    encoder._fo.write(data)


def xz_write_block(encoder, block_bytes, compression_level):
    """Write block in "xz" codec."""
    data = lzma.compress(block_bytes)
    encoder.write_long(len(data))
    encoder._fo.write(data)


BLOCK_WRITERS = {
    "null": null_write_block,
    "deflate": deflate_write_block,
    "bzip2": bzip2_write_block,
    "xz": xz_write_block,
}


def _missing_codec_lib(codec, library):
    def missing(encoder, block_bytes, compression_level):
        raise ValueError(
            f"{codec} codec is supported but you need to install {library}"
        )

    return missing


def snappy_write_block(encoder, block_bytes, compression_level):
    """Write block in "snappy" codec."""
    data = snappy.compress(block_bytes)
    encoder.write_long(len(data) + 4)  # for CRC
    encoder._fo.write(data)
    encoder.write_crc32(block_bytes)


try:
    import snappy
except ImportError:
    BLOCK_WRITERS["snappy"] = _missing_codec_lib("snappy", "python-snappy")
else:
    BLOCK_WRITERS["snappy"] = snappy_write_block


def zstandard_write_block(encoder, block_bytes, compression_level):
    """Write block in "zstandard" codec."""
    data = zstd.ZstdCompressor().compress(block_bytes)
    encoder.write_long(len(data))
    encoder._fo.write(data)


try:
    import zstandard as zstd
except ImportError:
    BLOCK_WRITERS["zstandard"] = _missing_codec_lib("zstandard", "zstandard")
else:
    BLOCK_WRITERS["zstandard"] = zstandard_write_block


def lz4_write_block(encoder, block_bytes, compression_level):
    """Write block in "lz4" codec."""
    data = lz4.block.compress(block_bytes)
    encoder.write_long(len(data))
    encoder._fo.write(data)


try:
    import lz4.block
except ImportError:
    BLOCK_WRITERS["lz4"] = _missing_codec_lib("lz4", "lz4")
else:
    BLOCK_WRITERS["lz4"] = lz4_write_block


class GenericWriter(object):
    def __init__(self, schema, metadata=None, validator=None):
        self._named_schemas = {}
        self.schema = parse_schema(schema, _named_schemas=self._named_schemas)
        self.validate_fn = _validate if validator is True else validator
        self.metadata = metadata or {}

        if isinstance(schema, dict):
            schema = {
                key: value
                for key, value in schema.items()
                if key not in ("__fastavro_parsed", "__named_schemas")
            }
        elif isinstance(schema, list):
            schemas = []
            for s in schema:
                if isinstance(s, dict):
                    schemas.append(
                        {
                            key: value
                            for key, value in s.items()
                            if key
                            not in (
                                "__fastavro_parsed",
                                "__named_schemas",
                            )
                        }
                    )
                else:
                    schemas.append(s)
            schema = schemas

        self.metadata["avro.schema"] = json.dumps(schema)


class Writer(GenericWriter):
    def __init__(
        self,
        fo,
        schema,
        codec="null",
        sync_interval=1000 * SYNC_SIZE,
        metadata=None,
        validator=None,
        sync_marker=None,
        compression_level=None,
    ):
        GenericWriter.__init__(self, schema, metadata, validator)

        self.metadata["avro.codec"] = codec
        if isinstance(fo, BinaryEncoder):
            self.encoder = fo
        else:
            self.encoder = BinaryEncoder(fo)
        self.io = BinaryEncoder(BytesIO())
        self.block_count = 0
        self.sync_interval = sync_interval
        self.compression_level = compression_level

        if _is_appendable(self.encoder._fo):
            # Seed to the beginning to read the header
            self.encoder._fo.seek(0)
            avro_reader = reader(self.encoder._fo)
            header = avro_reader._header

            file_writer_schema = parse_schema(avro_reader.writer_schema)
            if self.schema != file_writer_schema:
                raise ValueError(
                    f"Provided schema {self.schema} does not match "
                    + f"file writer_schema {file_writer_schema}"
                )

            codec = avro_reader.metadata.get("avro.codec", "null")

            self.sync_marker = header["sync"]

            # Seek to the end of the file
            self.encoder._fo.seek(0, 2)

            self.block_writer = BLOCK_WRITERS[codec]
        else:
            self.sync_marker = sync_marker or urandom(SYNC_SIZE)

            try:
                self.block_writer = BLOCK_WRITERS[codec]
            except KeyError:
                raise ValueError(f"unrecognized codec: {codec}")

            write_header(self.encoder, self.metadata, self.sync_marker)

    def dump(self):
        self.encoder.write_long(self.block_count)
        self.block_writer(self.encoder, self.io._fo.getvalue(), self.compression_level)
        self.encoder._fo.write(self.sync_marker)
        self.io._fo.truncate(0)
        self.io._fo.seek(0, SEEK_SET)
        self.block_count = 0

    def write(self, record):
        if self.validate_fn:
            self.validate_fn(record, self.schema, self._named_schemas)
        write_data(self.io, record, self.schema, self._named_schemas, "")
        self.block_count += 1
        if self.io._fo.tell() >= self.sync_interval:
            self.dump()

    def write_block(self, block):
        # Clear existing block if there are any records pending
        if self.io._fo.tell() or self.block_count > 0:
            self.dump()
        self.encoder.write_long(block.num_records)
        self.block_writer(self.encoder, block.bytes_.getvalue(), self.compression_level)
        self.encoder._fo.write(self.sync_marker)

    def flush(self):
        if self.io._fo.tell() or self.block_count > 0:
            self.dump()
        self.encoder._fo.flush()


class JSONWriter(GenericWriter):
    def __init__(
        self,
        fo,
        schema,
        codec="null",
        sync_interval=1000 * SYNC_SIZE,
        metadata=None,
        validator=None,
        sync_marker=None,
        codec_compression_level=None,
    ):
        GenericWriter.__init__(self, schema, metadata, validator)

        self.encoder = fo
        self.encoder.configure(self.schema, self._named_schemas)

    def write(self, record):
        if self.validate_fn:
            self.validate_fn(record, self.schema, self._named_schemas)
        write_data(self.encoder, record, self.schema, self._named_schemas, "")

    def flush(self):
        self.encoder.flush()


def writer(
    fo,
    schema,
    records,
    codec="null",
    sync_interval=1000 * SYNC_SIZE,
    metadata=None,
    validator=None,
    sync_marker=None,
    codec_compression_level=None,
):
    """Write records to fo (stream) according to schema

    Parameters
    ----------
    fo: file-like
        Output stream
    schema: dict
        Writer schema
    records: iterable
        Records to write. This is commonly a list of the dictionary
        representation of the records, but it can be any iterable
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
    sync_marker: bytes, optional
        A byte string used as the avro sync marker. If not provided, a random
        byte string will be used.
    codec_compression_level: int, optional
        Compression level to use with the specified codec (if the codec
        supports it)


    Example::

        from fastavro import writer, parse_schema

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
        parsed_schema = parse_schema(schema)

        records = [
            {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
            {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
            {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
            {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
        ]

        with open('weather.avro', 'wb') as out:
            writer(out, parsed_schema, records)

    The `fo` argument is a file-like object so another common example usage
    would use an `io.BytesIO` object like so::

        from io import BytesIO
        from fastavro import writer

        fo = BytesIO()
        writer(fo, schema, records)

    Given an existing avro file, it's possible to append to it by re-opening
    the file in `a+b` mode. If the file is only opened in `ab` mode, we aren't
    able to read some of the existing header information and an error will be
    raised. For example::

        # Write initial records
        with open('weather.avro', 'wb') as out:
            writer(out, parsed_schema, records)

        # Write some more records
        with open('weather.avro', 'a+b') as out:
            writer(out, parsed_schema, more_records)
    """
    # Sanity check that records is not a single dictionary (as that is a common
    # mistake and the exception that gets raised is not helpful)
    if isinstance(records, dict):
        raise ValueError('"records" argument should be an iterable, not dict')

    if isinstance(fo, AvroJSONEncoder):
        writer_class = JSONWriter
    else:
        # Assume a binary IO if an encoder isn't given
        writer_class = Writer
        fo = BinaryEncoder(fo)

    output = writer_class(
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

        parsed_schema = fastavro.parse_schema(schema)
        with open('file.avro', 'rb') as fp:
            fastavro.schemaless_writer(fp, parsed_schema, record)

    Note: The ``schemaless_writer`` can only write a single record.
    """
    named_schemas = {}
    schema = parse_schema(schema, _named_schemas=named_schemas)

    encoder = BinaryEncoder(fo)
    write_data(encoder, record, schema, named_schemas, "")
    encoder.flush()
