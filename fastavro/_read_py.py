# cython: auto_cpdef=True

"""Python code for reading AVRO files"""

# This code is a modified version of the code at
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/ which is under
# Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)

from io import BytesIO
from struct import error as StructError
import bz2
import lzma
import zlib
from datetime import datetime, time, date, timezone, timedelta
from decimal import Context
from uuid import UUID

import json

from .io.binary_decoder import BinaryDecoder
from .io.json_decoder import AvroJSONDecoder
from .schema import extract_record_type, extract_logical_type, parse_schema
from ._read_common import (
    SchemaResolutionError,
    MAGIC,
    SYNC_SIZE,
    HEADER_SCHEMA,
    missing_codec_lib,
    NAMED_TYPES,
)
from .const import (
    MCS_PER_HOUR,
    MCS_PER_MINUTE,
    MCS_PER_SECOND,
    MLS_PER_HOUR,
    MLS_PER_MINUTE,
    MLS_PER_SECOND,
    DAYS_SHIFT,
)

MASK = 0xFF
AVRO_TYPES = {
    "boolean",
    "bytes",
    "double",
    "float",
    "int",
    "long",
    "null",
    "string",
    "fixed",
    "enum",
    "record",
    "error",
    "array",
    "map",
    "union",
    "request",
    "error_union",
}

decimal_context = Context()
epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
epoch_naive = datetime(1970, 1, 1)


def match_types(writer_type, reader_type):
    if isinstance(writer_type, list) or isinstance(reader_type, list):
        return True
    if isinstance(writer_type, dict) or isinstance(reader_type, dict):
        try:
            return match_schemas(writer_type, reader_type)
        except SchemaResolutionError:
            return False
    if writer_type == reader_type:
        return True
    # promotion cases
    elif writer_type == "int" and reader_type in ["long", "float", "double"]:
        return True
    elif writer_type == "long" and reader_type in ["float", "double"]:
        return True
    elif writer_type == "float" and reader_type == "double":
        return True
    elif writer_type == "string" and reader_type == "bytes":
        return True
    elif writer_type == "bytes" and reader_type == "string":
        return True
    return False


def match_schemas(w_schema, r_schema):
    error_msg = f"Schema mismatch: {w_schema} is not {r_schema}"
    if isinstance(w_schema, list):
        # If the writer is a union, checks will happen in read_union after the
        # correct schema is known
        return r_schema
    elif isinstance(r_schema, list):
        # If the reader is a union, ensure one of the new schemas is the same
        # as the writer
        for schema in r_schema:
            if match_types(w_schema, schema):
                return schema
        else:
            raise SchemaResolutionError(error_msg)
    else:
        # Check for dicts as primitive types are just strings
        if isinstance(w_schema, dict):
            w_type = w_schema["type"]
        else:
            w_type = w_schema
        if isinstance(r_schema, dict):
            r_type = r_schema["type"]
        else:
            r_type = r_schema

        if w_type == r_type == "map":
            if match_types(w_schema["values"], r_schema["values"]):
                return r_schema
        elif w_type == r_type == "array":
            if match_types(w_schema["items"], r_schema["items"]):
                return r_schema
        elif w_type in NAMED_TYPES and r_type in NAMED_TYPES:
            if w_type == r_type == "fixed" and w_schema["size"] != r_schema["size"]:
                raise SchemaResolutionError(
                    f"Schema mismatch: {w_schema} size is different than {r_schema} size"
                )
            if w_schema["name"] == r_schema["name"] or w_schema["name"] in r_schema.get(
                "aliases", []
            ):
                return r_schema
        elif match_types(w_type, r_type):
            return r_schema
        raise SchemaResolutionError(error_msg)


def read_null(
    decoder,
    writer_schema=None,
    named_schemas=None,
    reader_schema=None,
    return_record_name=False,
):
    return decoder.read_null()


def skip_null(decoder, writer_schema=None, named_schemas=None):
    decoder.read_null()


def read_boolean(
    decoder,
    writer_schema=None,
    named_schemas=None,
    reader_schema=None,
    return_record_name=False,
):
    return decoder.read_boolean()


def skip_boolean(decoder, writer_schema=None, named_schemas=None):
    decoder.read_boolean()


def read_timestamp_millis(data, writer_schema=None, reader_schema=None):
    # Cannot use datetime.fromtimestamp: https://bugs.python.org/issue36439
    return epoch + timedelta(microseconds=data * 1000)


def read_local_timestamp_millis(
    data: int, writer_schema=None, reader_schema=None
) -> datetime:
    # Cannot use datetime.fromtimestamp: https://bugs.python.org/issue36439
    return epoch_naive + timedelta(microseconds=data * 1000)


def read_timestamp_micros(data, writer_schema=None, reader_schema=None):
    # Cannot use datetime.fromtimestamp: https://bugs.python.org/issue36439
    return epoch + timedelta(microseconds=data)


def read_local_timestamp_micros(
    data: int, writer_schema=None, reader_schema=None
) -> datetime:
    # Cannot use datetime.fromtimestamp: https://bugs.python.org/issue36439
    return epoch_naive + timedelta(microseconds=data)


def read_date(data, writer_schema=None, reader_schema=None):
    return date.fromordinal(data + DAYS_SHIFT)


def read_uuid(data, writer_schema=None, reader_schema=None):
    return UUID(data)


def read_time_millis(data, writer_schema=None, reader_schema=None):
    h = int(data / MLS_PER_HOUR)
    m = int(data / MLS_PER_MINUTE) % 60
    s = int(data / MLS_PER_SECOND) % 60
    mls = int(data % MLS_PER_SECOND) * 1000
    return time(h, m, s, mls)


def read_time_micros(data, writer_schema=None, reader_schema=None):
    h = int(data / MCS_PER_HOUR)
    m = int(data / MCS_PER_MINUTE) % 60
    s = int(data / MCS_PER_SECOND) % 60
    mcs = data % MCS_PER_SECOND
    return time(h, m, s, mcs)


def read_decimal(data, writer_schema=None, reader_schema=None):
    scale = writer_schema.get("scale", 0)
    precision = writer_schema["precision"]

    unscaled_datum = int.from_bytes(data, byteorder="big", signed=True)

    decimal_context.prec = precision
    return decimal_context.create_decimal(unscaled_datum).scaleb(
        -scale, decimal_context
    )


def read_int(
    decoder,
    writer_schema=None,
    named_schemas=None,
    reader_schema=None,
    return_record_name=False,
):
    return decoder.read_int()


def skip_int(decoder, writer_schema=None, named_schemas=None):
    decoder.read_int()


def read_long(
    decoder,
    writer_schema=None,
    named_schemas=None,
    reader_schema=None,
    return_record_name=False,
):
    return decoder.read_long()


def skip_long(decoder, writer_schema=None, named_schemas=None):
    decoder.read_long()


def read_float(
    decoder,
    writer_schema=None,
    named_schemas=None,
    reader_schema=None,
    return_record_name=False,
):
    return decoder.read_float()


def skip_float(decoder, writer_schema=None, named_schemas=None):
    decoder.read_float()


def read_double(
    decoder,
    writer_schema=None,
    named_schemas=None,
    reader_schema=None,
    return_record_name=False,
):
    return decoder.read_double()


def skip_double(decoder, writer_schema=None, named_schemas=None):
    decoder.read_double()


def read_bytes(
    decoder,
    writer_schema=None,
    named_schemas=None,
    reader_schema=None,
    return_record_name=False,
):
    return decoder.read_bytes()


def skip_bytes(decoder, writer_schema=None, named_schemas=None):
    decoder.read_bytes()


def read_utf8(
    decoder,
    writer_schema=None,
    named_schemas=None,
    reader_schema=None,
    return_record_name=False,
):
    return decoder.read_utf8()


def skip_utf8(decoder, writer_schema=None, named_schemas=None):
    decoder.read_utf8()


def read_fixed(
    decoder,
    writer_schema,
    named_schemas=None,
    reader_schema=None,
    return_record_name=False,
):
    size = writer_schema["size"]
    return decoder.read_fixed(size)


def skip_fixed(decoder, writer_schema, named_schemas=None):
    size = writer_schema["size"]
    decoder.read_fixed(size)


def read_enum(
    decoder,
    writer_schema,
    named_schemas,
    reader_schema=None,
    return_record_name=False,
):
    symbol = writer_schema["symbols"][decoder.read_enum()]
    if reader_schema and symbol not in reader_schema["symbols"]:
        default = reader_schema.get("default")
        if default:
            return default
        else:
            symlist = reader_schema["symbols"]
            msg = f"{symbol} not found in reader symbol list {symlist}"
            raise SchemaResolutionError(msg)
    return symbol


def skip_enum(decoder, writer_schema, named_schemas):
    decoder.read_enum()


def read_array(
    decoder,
    writer_schema,
    named_schemas,
    reader_schema=None,
    return_record_name=False,
):
    if reader_schema:

        def item_reader(decoder, w_schema, r_schema, return_record_name):
            return read_data(
                decoder,
                w_schema["items"],
                named_schemas,
                r_schema["items"],
                return_record_name,
            )

    else:

        def item_reader(decoder, w_schema, _, return_record_name):
            return read_data(
                decoder,
                w_schema["items"],
                named_schemas,
                None,
                return_record_name,
            )

    read_items = []

    decoder.read_array_start()

    for item in decoder.iter_array():
        read_items.append(
            item_reader(decoder, writer_schema, reader_schema, return_record_name)
        )

    decoder.read_array_end()

    return read_items


def skip_array(decoder, writer_schema, named_schemas):
    decoder.read_array_start()

    for item in decoder.iter_array():
        skip_data(decoder, writer_schema["items"], named_schemas)

    decoder.read_array_end()


def read_map(
    decoder,
    writer_schema,
    named_schemas,
    reader_schema=None,
    return_record_name=False,
):
    if reader_schema:

        def item_reader(decoder, w_schema, r_schema):
            return read_data(
                decoder,
                w_schema["values"],
                named_schemas,
                r_schema["values"],
                return_record_name,
            )

    else:

        def item_reader(decoder, w_schema, _):
            return read_data(
                decoder,
                w_schema["values"],
                named_schemas,
                None,
                return_record_name,
            )

    read_items = {}

    decoder.read_map_start()

    for item in decoder.iter_map():
        key = decoder.read_utf8()
        read_items[key] = item_reader(decoder, writer_schema, reader_schema)

    decoder.read_map_end()

    return read_items


def skip_map(decoder, writer_schema, named_schemas):
    decoder.read_map_start()

    for item in decoder.iter_map():
        decoder.read_utf8()
        skip_data(decoder, writer_schema["values"], named_schemas)

    decoder.read_map_end()


def read_union(
    decoder,
    writer_schema,
    named_schemas,
    reader_schema=None,
    return_record_name=False,
):
    # schema resolution
    index = decoder.read_index()
    idx_schema = writer_schema[index]

    if reader_schema:
        # Handle case where the reader schema is just a single type (not union)
        if not isinstance(reader_schema, list):
            if match_types(idx_schema, reader_schema):
                return read_data(
                    decoder,
                    idx_schema,
                    named_schemas,
                    reader_schema,
                    return_record_name,
                )
        else:
            for schema in reader_schema:
                if match_types(idx_schema, schema):
                    return read_data(
                        decoder,
                        idx_schema,
                        named_schemas,
                        schema,
                        return_record_name,
                    )
        msg = f"schema mismatch: {writer_schema} not found in {reader_schema}"
        raise SchemaResolutionError(msg)
    else:
        if return_record_name and extract_record_type(idx_schema) == "record":
            return (
                idx_schema["name"],
                read_data(
                    decoder,
                    idx_schema,
                    named_schemas,
                    None,
                    return_record_name,
                ),
            )
        elif return_record_name and extract_record_type(idx_schema) not in AVRO_TYPES:
            # idx_schema is a named type
            return (
                named_schemas[idx_schema]["name"],
                read_data(decoder, idx_schema, named_schemas, None, return_record_name),
            )
        else:
            return read_data(decoder, idx_schema, named_schemas)


def skip_union(decoder, writer_schema, named_schemas):
    # schema resolution
    index = decoder.read_index()
    skip_data(decoder, writer_schema[index], named_schemas)


def read_record(
    decoder,
    writer_schema,
    named_schemas,
    reader_schema=None,
    return_record_name=False,
):
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
        for field in writer_schema["fields"]:
            record[field["name"]] = read_data(
                decoder, field["type"], named_schemas, None, return_record_name
            )
    else:
        readers_field_dict = {}
        aliases_field_dict = {}
        for f in reader_schema["fields"]:
            readers_field_dict[f["name"]] = f
            for alias in f.get("aliases", []):
                aliases_field_dict[alias] = f

        for field in writer_schema["fields"]:
            readers_field = readers_field_dict.get(
                field["name"],
                aliases_field_dict.get(field["name"]),
            )
            if readers_field:
                record[readers_field["name"]] = read_data(
                    decoder,
                    field["type"],
                    named_schemas,
                    readers_field["type"],
                    return_record_name,
                )
            else:
                skip_data(decoder, field["type"], named_schemas)

        # fill in default values
        if len(readers_field_dict) > len(record):
            writer_fields = [f["name"] for f in writer_schema["fields"]]
            for f_name, field in readers_field_dict.items():
                if f_name not in writer_fields and f_name not in record:
                    if "default" in field:
                        record[field["name"]] = field["default"]
                    else:
                        msg = f'No default value for {field["name"]}'
                        raise SchemaResolutionError(msg)

    return record


def skip_record(decoder, writer_schema, named_schemas):
    for field in writer_schema["fields"]:
        skip_data(decoder, field["type"], named_schemas)


LOGICAL_READERS = {
    "long-timestamp-millis": read_timestamp_millis,
    "long-local-timestamp-millis": read_local_timestamp_millis,
    "long-timestamp-micros": read_timestamp_micros,
    "long-local-timestamp-micros": read_local_timestamp_micros,
    "int-date": read_date,
    "bytes-decimal": read_decimal,
    "fixed-decimal": read_decimal,
    "string-uuid": read_uuid,
    "int-time-millis": read_time_millis,
    "long-time-micros": read_time_micros,
}

READERS = {
    "null": read_null,
    "boolean": read_boolean,
    "string": read_utf8,
    "int": read_int,
    "long": read_long,
    "float": read_float,
    "double": read_double,
    "bytes": read_bytes,
    "fixed": read_fixed,
    "enum": read_enum,
    "array": read_array,
    "map": read_map,
    "union": read_union,
    "error_union": read_union,
    "record": read_record,
    "error": read_record,
    "request": read_record,
}

SKIPS = {
    "null": skip_null,
    "boolean": skip_boolean,
    "string": skip_utf8,
    "int": skip_int,
    "long": skip_long,
    "float": skip_float,
    "double": skip_double,
    "bytes": skip_bytes,
    "fixed": skip_fixed,
    "enum": skip_enum,
    "array": skip_array,
    "map": skip_map,
    "union": skip_union,
    "error_union": skip_union,
    "record": skip_record,
    "error": skip_record,
    "request": skip_record,
}


def maybe_promote(data, writer_type, reader_type):
    if writer_type == "int":
        # No need to promote to long since they are the same type in Python
        if reader_type == "float" or reader_type == "double":
            return float(data)
    if writer_type == "long":
        if reader_type == "float" or reader_type == "double":
            return float(data)
    if writer_type == "string" and reader_type == "bytes":
        return data.encode()
    if writer_type == "bytes" and reader_type == "string":
        return data.decode()
    return data


def read_data(
    decoder, writer_schema, named_schemas, reader_schema=None, return_record_name=False
):
    """Read data from file object according to schema."""

    record_type = extract_record_type(writer_schema)

    if reader_schema and record_type in AVRO_TYPES:
        # If the schemas are the same, set the reader schema to None so that no
        # schema resolution is done for this call or future recursive calls
        if writer_schema == reader_schema:
            reader_schema = None
        else:
            reader_schema = match_schemas(writer_schema, reader_schema)

    reader_fn = READERS.get(record_type)
    if reader_fn:
        try:
            data = reader_fn(
                decoder,
                writer_schema,
                named_schemas,
                reader_schema,
                return_record_name,
            )
        except StructError:
            raise EOFError(f"cannot read {record_type} from {decoder.fo}")

        if "logicalType" in writer_schema:
            logical_type = extract_logical_type(writer_schema)
            fn = LOGICAL_READERS.get(logical_type)
            if fn:
                return fn(data, writer_schema, reader_schema)

        if reader_schema is not None:
            return maybe_promote(data, record_type, extract_record_type(reader_schema))
        else:
            return data
    else:
        return read_data(
            decoder,
            named_schemas[record_type],
            named_schemas,
            named_schemas.get(reader_schema),
            return_record_name,
        )


def skip_data(decoder, writer_schema, named_schemas):
    record_type = extract_record_type(writer_schema)

    reader_fn = SKIPS.get(record_type)
    if reader_fn:
        reader_fn(decoder, writer_schema, named_schemas)
    else:
        skip_data(decoder, named_schemas[record_type], named_schemas)


def skip_sync(fo, sync_marker):
    """Skip an expected sync marker, complaining if it doesn't match"""
    if fo.read(SYNC_SIZE) != sync_marker:
        raise ValueError("expected sync marker not found")


def null_read_block(decoder):
    """Read block in "null" codec."""
    return BytesIO(decoder.read_bytes())


def deflate_read_block(decoder):
    """Read block in "deflate" codec."""
    data = decoder.read_bytes()
    # -15 is the log of the window size; negative indicates "raw" (no
    # zlib headers) decompression.  See zlib.h.
    return BytesIO(zlib.decompress(data, -15))


def bzip2_read_block(decoder):
    """Read block in "bzip2" codec."""
    data = decoder.read_bytes()
    return BytesIO(bz2.decompress(data))


def xz_read_block(decoder):
    length = read_long(decoder)
    data = decoder.read_fixed(length)
    return BytesIO(lzma.decompress(data))


BLOCK_READERS = {
    "null": null_read_block,
    "deflate": deflate_read_block,
    "bzip2": bzip2_read_block,
    "xz": xz_read_block,
}


def snappy_read_block(decoder):
    length = read_long(decoder)
    data = decoder.read_fixed(length - 4)
    decoder.read_fixed(4)  # CRC
    return BytesIO(snappy.decompress(data))


try:
    import snappy
except ImportError:
    BLOCK_READERS["snappy"] = missing_codec_lib("snappy", "python-snappy")
else:
    BLOCK_READERS["snappy"] = snappy_read_block


def zstandard_read_block(decoder):
    length = read_long(decoder)
    data = decoder.read_fixed(length)
    return BytesIO(zstd.ZstdDecompressor().decompress(data))


try:
    import zstandard as zstd
except ImportError:
    BLOCK_READERS["zstandard"] = missing_codec_lib("zstandard", "zstandard")
else:
    BLOCK_READERS["zstandard"] = zstandard_read_block


def lz4_read_block(decoder):
    length = read_long(decoder)
    data = decoder.read_fixed(length)
    return BytesIO(lz4.block.decompress(data))


try:
    import lz4.block
except ImportError:
    BLOCK_READERS["lz4"] = missing_codec_lib("lz4", "lz4")
else:
    BLOCK_READERS["lz4"] = lz4_read_block


def _iter_avro_records(
    decoder,
    header,
    codec,
    writer_schema,
    named_schemas,
    reader_schema,
    return_record_name=False,
):
    """Return iterator over avro records."""
    sync_marker = header["sync"]

    read_block = BLOCK_READERS.get(codec)
    if not read_block:
        raise ValueError(f"Unrecognized codec: {codec}")

    block_count = 0
    while True:
        try:
            block_count = decoder.read_long()
        except StopIteration:
            return

        block_fo = read_block(decoder)

        for i in range(block_count):
            yield read_data(
                BinaryDecoder(block_fo),
                writer_schema,
                named_schemas,
                reader_schema,
                return_record_name,
            )

        skip_sync(decoder.fo, sync_marker)


def _iter_avro_blocks(
    decoder,
    header,
    codec,
    writer_schema,
    named_schemas,
    reader_schema,
    return_record_name=False,
):
    """Return iterator over avro blocks."""
    sync_marker = header["sync"]

    read_block = BLOCK_READERS.get(codec)
    if not read_block:
        raise ValueError(f"Unrecognized codec: {codec}")

    while True:
        offset = decoder.fo.tell()
        try:
            num_block_records = decoder.read_long()
        except StopIteration:
            return

        block_bytes = read_block(decoder)

        skip_sync(decoder.fo, sync_marker)

        size = decoder.fo.tell() - offset

        yield Block(
            block_bytes,
            num_block_records,
            codec,
            reader_schema,
            writer_schema,
            named_schemas,
            offset,
            size,
            return_record_name,
        )


class Block:
    """An avro block. Will yield records when iterated over

    .. attribute:: num_records

        Number of records in the block

    .. attribute:: writer_schema

        The schema used when writing

    .. attribute:: reader_schema

        The schema used when reading (if provided)

    .. attribute:: offset

        Offset of the block from the begining of the avro file

    .. attribute:: size

        Size of the block in bytes
    """

    def __init__(
        self,
        bytes_,
        num_records,
        codec,
        reader_schema,
        writer_schema,
        named_schemas,
        offset,
        size,
        return_record_name=False,
    ):
        self.bytes_ = bytes_
        self.num_records = num_records
        self.codec = codec
        self.reader_schema = reader_schema
        self.writer_schema = writer_schema
        self._named_schemas = named_schemas
        self.offset = offset
        self.size = size
        self.return_record_name = return_record_name

    def __iter__(self):
        for i in range(self.num_records):
            yield read_data(
                BinaryDecoder(self.bytes_),
                self.writer_schema,
                self._named_schemas,
                self.reader_schema,
                self.return_record_name,
            )

    def __str__(self):
        return (
            f"Avro block: {len(self.bytes_)} bytes, "
            + f"{self.num_records} records, "
            + f"codec: {self.codec}, position {self.offset}+{self.size}"
        )


class file_reader:
    def __init__(self, fo_or_decoder, reader_schema=None, return_record_name=False):
        if isinstance(fo_or_decoder, AvroJSONDecoder):
            self.decoder = fo_or_decoder
        else:
            # If a decoder was not provided, assume binary
            self.decoder = BinaryDecoder(fo_or_decoder)

        self._named_schemas = {}
        if reader_schema:
            self.reader_schema = parse_schema(
                reader_schema,
                _write_hint=False,
                _named_schemas=self._named_schemas,
            )
        else:
            self.reader_schema = None
        self.return_record_name = return_record_name
        self._elems = None

    def _read_header(self):
        try:
            self._header = read_data(
                self.decoder,
                HEADER_SCHEMA,
                self._named_schemas,
                None,
                self.return_record_name,
            )
        except StopIteration:
            raise ValueError("cannot read header - is it an avro file?")

        # `meta` values are bytes. So, the actual decoding has to be external.
        self.metadata = {k: v.decode() for k, v in self._header["meta"].items()}

        self._schema = json.loads(self.metadata["avro.schema"])
        self.codec = self.metadata.get("avro.codec", "null")

        # Always parse the writer schema since it might have named types that
        # need to be stored in self._named_types
        self.writer_schema = parse_schema(
            self._schema,
            _write_hint=False,
            _force=True,
            _named_schemas=self._named_schemas,
        )

    @property
    def schema(self):
        import warnings

        warnings.warn(
            "The 'schema' attribute is deprecated. Please use 'writer_schema'",
            DeprecationWarning,
        )
        return self._schema

    def __iter__(self):
        if not self._elems:
            raise NotImplementedError
        return self._elems

    def next(self):
        return next(self._elems)

    __next__ = next


class reader(file_reader):
    """Iterator over records in an avro file.

    Parameters
    ----------
    fo: file-like
        Input stream
    reader_schema: dict, optional
        Reader schema
    return_record_name: bool, optional
        If true, when reading a union of records, the result will be a tuple
        where the first value is the name of the record and the second value is
        the record itself


    Example::

        from fastavro import reader
        with open('some-file.avro', 'rb') as fo:
            avro_reader = reader(fo)
            for record in avro_reader:
                process_record(record)

    The `fo` argument is a file-like object so another common example usage
    would use an `io.BytesIO` object like so::

        from io import BytesIO
        from fastavro import writer, reader

        fo = BytesIO()
        writer(fo, schema, records)
        fo.seek(0)
        for record in reader(fo):
            process_record(record)

    .. attribute:: metadata

        Key-value pairs in the header metadata

    .. attribute:: codec

        The codec used when writing

    .. attribute:: writer_schema

        The schema used when writing

    .. attribute:: reader_schema

        The schema used when reading (if provided)
    """

    def __init__(self, fo, reader_schema=None, return_record_name=False):
        file_reader.__init__(self, fo, reader_schema, return_record_name)

        if isinstance(self.decoder, AvroJSONDecoder):
            self.decoder.configure(self.reader_schema, self._named_schemas)

            self.writer_schema = self.reader_schema
            self.reader_schema = None

            def _elems():
                while not self.decoder.done:
                    yield read_data(
                        self.decoder,
                        self.writer_schema,
                        self._named_schemas,
                        self.reader_schema,
                        self.return_record_name,
                    )
                    self.decoder.drain()

            self._elems = _elems()

        else:
            self._read_header()

            self._elems = _iter_avro_records(
                self.decoder,
                self._header,
                self.codec,
                self.writer_schema,
                self._named_schemas,
                self.reader_schema,
                self.return_record_name,
            )


class block_reader(file_reader):
    """Iterator over :class:`.Block` in an avro file.

    Parameters
    ----------
    fo: file-like
        Input stream
    reader_schema: dict, optional
        Reader schema
    return_record_name: bool, optional
        If true, when reading a union of records, the result will be a tuple
        where the first value is the name of the record and the second value is
        the record itself


    Example::

        from fastavro import block_reader
        with open('some-file.avro', 'rb') as fo:
            avro_reader = block_reader(fo)
            for block in avro_reader:
                process_block(block)

    .. attribute:: metadata

        Key-value pairs in the header metadata

    .. attribute:: codec

        The codec used when writing

    .. attribute:: writer_schema

        The schema used when writing

    .. attribute:: reader_schema

        The schema used when reading (if provided)
    """

    def __init__(self, fo, reader_schema=None, return_record_name=False):
        file_reader.__init__(self, fo, reader_schema, return_record_name)

        self._read_header()

        self._elems = _iter_avro_blocks(
            self.decoder,
            self._header,
            self.codec,
            self.writer_schema,
            self._named_schemas,
            self.reader_schema,
            self.return_record_name,
        )


def schemaless_reader(fo, writer_schema, reader_schema=None, return_record_name=False):
    """Reads a single record writen using the
    :meth:`~fastavro._write_py.schemaless_writer`

    Parameters
    ----------
    fo: file-like
        Input stream
    writer_schema: dict
        Schema used when calling schemaless_writer
    reader_schema: dict, optional
        If the schema has changed since being written then the new schema can
        be given to allow for schema migration
    return_record_name: bool, optional
        If true, when reading a union of records, the result will be a tuple
        where the first value is the name of the record and the second value is
        the record itself


    Example::

        parsed_schema = fastavro.parse_schema(schema)
        with open('file', 'rb') as fp:
            record = fastavro.schemaless_reader(fp, parsed_schema)

    Note: The ``schemaless_reader`` can only read a single record.
    """
    if writer_schema == reader_schema:
        # No need for the reader schema if they are the same
        reader_schema = None

    named_schemas = {}
    writer_schema = parse_schema(writer_schema, _named_schemas=named_schemas)

    if reader_schema:
        reader_schema = parse_schema(reader_schema)

    decoder = BinaryDecoder(fo)

    return read_data(
        decoder,
        writer_schema,
        named_schemas,
        reader_schema,
        return_record_name,
    )


def is_avro(path_or_buffer):
    """Return True if path (or buffer) points to an Avro file. This will only
    work for avro files that contain the normal avro schema header like those
    create from :func:`~fastavro._write_py.writer`. This function is not intended
    to be used with binary data created from
    :func:`~fastavro._write_py.schemaless_writer` since that does not include the
    avro header.

    Parameters
    ----------
    path_or_buffer: path to file or file-like object
        Path to file
    """
    if isinstance(path_or_buffer, str):
        fp = open(path_or_buffer, "rb")
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
