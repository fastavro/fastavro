from io import BytesIO
import math
import fastavro
from fastavro.io.binary_decoder import BinaryDecoder
from fastavro.read import _read as _reader, HEADER_SCHEMA, SchemaResolutionError
from fastavro.write import _write as _writer, Writer

import pytest

import copy
import datetime
import sys
import traceback
import zipfile
from collections import OrderedDict
from os.path import join, abspath, dirname, basename
from glob import iglob

data_dir = join(abspath(dirname(__file__)), "avro-files")

try:
    import snappy  # NOQA

    has_snappy = True
except ImportError:
    has_snappy = False

NO_DATA = {
    "class org.apache.avro.tool.TestDataFileTools.zerojsonvalues.avro",
    "testDataFileMeta.avro",
}


def roundtrip(schema, records, *, writer_kwargs={}, **reader_kwargs):
    new_file = BytesIO()
    fastavro.writer(new_file, schema, records, **writer_kwargs)
    new_file.seek(0)

    reader = fastavro.reader(new_file, **reader_kwargs)

    new_records = list(reader)
    return new_records


class NoSeekBytesIO:
    """Shim around BytesIO which blocks access to everything but read.
    Used to ensure seek and tell API isn't being depended on."""

    def __init__(self, *args):
        self.underlying = BytesIO(*args)

    def read(self, n):
        return self.underlying.read(n)

    def tell(self):
        raise AssertionError("fastavro reader should not depend on tell")

    def seek(self, *args):
        raise AssertionError("fastavro reader should not depend on seek")


def _test_files():
    for filename in iglob(join(data_dir, "*.avro")):
        if (not has_snappy) and ("snappy" in filename):
            continue
        yield filename


def remove_legacy_fields(schema):
    if "__fastavro_parsed" in schema:
        schema.pop("__fastavro_parsed")
    if "__named_schemas" in schema:
        schema.pop("__named_schemas")
    return schema


@pytest.mark.parametrize("filename", _test_files())
def test_file(filename):
    with open(filename, "rb") as fo:
        reader = fastavro.reader(fo)
        assert hasattr(reader, "writer_schema"), "no schema on file"

        if basename(filename) in NO_DATA:
            return

        records = list(reader)
        assert len(records) > 0, "no records found"

    new_file = BytesIO()
    fastavro.writer(new_file, reader.writer_schema, records, reader.codec)
    new_file_bytes = new_file.getvalue()

    new_file = NoSeekBytesIO(new_file_bytes)
    new_reader = fastavro.reader(new_file)
    assert hasattr(new_reader, "writer_schema"), "schema wasn't written"
    assert new_reader.writer_schema == remove_legacy_fields(
        copy.deepcopy(reader.writer_schema)
    )
    assert new_reader.codec == reader.codec
    new_records = list(new_reader)

    assert new_records == records

    # Test schema migration with the same schema
    new_file = NoSeekBytesIO(new_file_bytes)
    schema_migration_reader = fastavro.reader(new_file, reader.writer_schema)
    assert schema_migration_reader.reader_schema == reader.writer_schema
    new_records = list(schema_migration_reader)

    assert new_records == records


def test_not_avro():
    with pytest.raises(ValueError):
        with open(__file__, "rb") as fo:
            fastavro.reader(fo)


def test_parse_schema_rejects_undleclared_name():
    try:
        fastavro.parse_schema(
            {
                "type": "record",
                "name": "test_parse_schema_rejects_undleclared_name",
                "fields": [
                    {
                        "name": "left",
                        "type": "Thinger",
                    }
                ],
            }
        )
        assert False, "Never raised"
    except fastavro.schema.UnknownType as e:
        assert "Thinger" == e.name


def test_parse_schema_rejects_unordered_references():
    try:
        fastavro.parse_schema(
            {
                "type": "record",
                "name": "test_parse_schema_rejects_unordered_references",
                "fields": [
                    {"name": "left", "type": "Thinger"},
                    {
                        "name": "right",
                        "type": {
                            "type": "record",
                            "name": "Thinger",
                            "fields": [{"name": "the_thing", "type": "string"}],
                        },
                    },
                ],
            }
        )
        assert False, "Never raised"
    except fastavro.schema.UnknownType as e:
        assert "Thinger" == e.name


def test_parse_schema_accepts_nested_namespaces():
    parsed_schema = fastavro.parse_schema(
        {
            "namespace": "com.example",
            "name": "Outer",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": {
                        "type": "record",
                        "name": "Inner",
                        "fields": [{"name": "the_thing", "type": "string"}],
                    },
                },
                {
                    "name": "b",
                    # This should resolve to com.example.Inner because of the
                    # `namespace` of the enclosing record.
                    "type": "Inner",
                },
                {"name": "b", "type": "com.example.Inner"},
            ],
        }
    )
    assert "com.example.Inner" == parsed_schema["fields"][0]["type"]["name"]
    assert "com.example.Inner" == parsed_schema["fields"][1]["type"]


def test_parse_schema_resolves_references_from_unions():
    parsed_schema = fastavro.parse_schema(
        {
            "namespace": "com.other",
            "name": "Outer",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "Inner",
                            "fields": [{"name": "the_thing", "type": "string"}],
                        },
                    ],
                },
                {
                    "name": "b",
                    # This should resolve to com.example.Inner because of the
                    # `namespace` of the enclosing record.
                    "type": ["null", "Inner"],
                },
            ],
        }
    )
    assert "com.other.Inner" == parsed_schema["fields"][1]["type"][1]


def test_parse_schema_accepts_nested_records_from_arrays():
    parsed_schema = fastavro.parse_schema(
        {
            "fields": [
                {
                    "type": {
                        "items": {
                            "type": "record",
                            "fields": [{"type": "string", "name": "text"}],
                            "name": "Nested",
                        },
                        "type": "array",
                    },
                    "name": "multiple",
                },
                {"type": {"type": "array", "items": "Nested"}, "name": "single"},
            ],
            "type": "record",
            "name": "test_parse_schema_accepts_nested_records_from_arrays",
        }
    )
    assert "Nested" == parsed_schema["fields"][1]["type"]["items"]


def test_compose_schemas():
    schema_path = join(data_dir, "Parent.avsc")
    fastavro.schema.load_schema(schema_path)


def test_reading_after_writing_with_load_schema():
    schema_path = join(data_dir, "Parent.avsc")
    schema = fastavro.schema.load_schema(schema_path)

    records = [{"child": {}, "child1": {}}]

    new_file = BytesIO()
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)

    reader = fastavro.reader(new_file)
    new_records = list(reader)
    assert new_records == records


def test_missing_schema():
    schema_path = join(data_dir, "ParentMissingChild.avsc")
    with pytest.raises(fastavro.schema.UnknownType):
        fastavro.schema.load_schema(schema_path)


def test_default_values():
    schema = {
        "type": "record",
        "name": "test_default_values",
        "fields": [
            {"name": "default_field", "type": "string", "default": "default_value"}
        ],
    }
    records = [{}]

    new_records = roundtrip(schema, records)
    assert new_records == [{"default_field": "default_value"}]


def test_nullable_values():
    schema = {
        "type": "record",
        "name": "test_nullable_values",
        "fields": [
            {"name": "nullable_field", "type": ["string", "null"]},
            {"name": "field", "type": "string"},
        ],
    }
    records = [{"field": "val"}, {"field": "val", "nullable_field": "no_null"}]

    new_records = roundtrip(schema, records)
    assert new_records == [
        {"nullable_field": None, "field": "val"},
        {"nullable_field": "no_null", "field": "val"},
    ]


def test_metadata():
    schema = {"type": "record", "name": "test_metadata", "fields": []}

    new_file = BytesIO()
    records = [{}]
    metadata = {"key": "value"}
    fastavro.writer(new_file, schema, records, metadata=metadata)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    assert new_reader.metadata["key"] == metadata["key"]


def test_write_union_shortcut():
    schema = {
        "type": "record",
        "name": "A",
        "fields": [
            {
                "name": "a",
                "type": [
                    {
                        "type": "record",
                        "name": "B",
                        "fields": [{"name": "b", "type": "string"}],
                    },
                    {
                        "type": "record",
                        "name": "C",
                        "fields": [{"name": "c", "type": "string"}],
                    },
                ],
            }
        ],
    }

    records = [{"a": ("B", {"b": "test"})}]

    assert [{"a": {"b": "test"}}] == roundtrip(schema, records)


def test_repo_caching_issue():
    schema = {
        "type": "record",
        "name": "B",
        "fields": [
            {
                "name": "b",
                "type": {
                    "type": "record",
                    "name": "C",
                    "fields": [{"name": "c", "type": "string"}],
                },
            }
        ],
    }

    records = [{"b": {"c": "test"}}]

    assert records == roundtrip(schema, records)

    other_schema = {
        "name": "A",
        "type": "record",
        "fields": [
            {
                "name": "a",
                "type": {
                    "type": "record",
                    "name": "B",
                    "fields": [
                        {
                            "name": "b",
                            "type": {
                                "type": "record",
                                "name": "C",
                                "fields": [{"name": "c", "type": "int"}],
                            },
                        }
                    ],
                },
            },
            {"name": "aa", "type": "B"},
        ],
    }

    records = [{"a": {"b": {"c": 1}}, "aa": {"b": {"c": 2}}}]

    assert records == roundtrip(other_schema, records)


def test_schema_migration_remove_field():
    schema = {
        "type": "record",
        "name": "test_schema_migration_remove_field",
        "fields": [
            {
                "name": "test",
                "type": "string",
            }
        ],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_remove_field",
        "fields": [],
    }

    new_file = BytesIO()
    records = [{"test": "test"}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == [{}]


def test_schema_migration_add_default_field():
    schema = {
        "type": "record",
        "name": "test_schema_migration_add_default_field",
        "fields": [],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_add_default_field",
        "fields": [
            {
                "name": "test",
                "type": "string",
                "default": "default",
            }
        ],
    }

    new_file = BytesIO()
    records = [{}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == [{"test": "default"}]


def test_schema_migration_type_promotion():
    schema = {
        "type": "record",
        "name": "test_schema_migration_type_promotion",
        "fields": [
            {
                "name": "test",
                "type": ["string", "int"],
            }
        ],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_type_promotion",
        "fields": [
            {
                "name": "test",
                "type": ["float", "string"],
            }
        ],
    }

    new_file = BytesIO()
    records = [{"test": 1}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_maps_with_union_promotion():
    schema = {
        "type": "record",
        "name": "test_schema_migration_maps_with_union_promotion",
        "fields": [
            {
                "name": "test",
                "type": {"type": "map", "values": ["string", "int"]},
            }
        ],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_maps_with_union_promotion",
        "fields": [
            {
                "name": "test",
                "type": {"type": "map", "values": ["string", "long"]},
            }
        ],
    }

    new_file = BytesIO()
    records = [{"test": {"foo": 1}}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_array_with_union_promotion():
    schema = {
        "type": "record",
        "name": "test_schema_migration_array_with_union_promotion",
        "fields": [
            {
                "name": "test",
                "type": {"type": "array", "items": ["boolean", "long"]},
            }
        ],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_array_with_union_promotion",
        "fields": [
            {
                "name": "test",
                "type": {"type": "array", "items": ["string", "float"]},
            }
        ],
    }

    new_file = BytesIO()
    records = [{"test": [1, 2, 3]}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_writer_union():
    schema = {
        "type": "record",
        "name": "test_schema_migration_writer_union",
        "fields": [{"name": "test", "type": ["string", "int"]}],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_writer_union",
        "fields": [{"name": "test", "type": "int"}],
    }

    new_file = BytesIO()
    records = [{"test": 1}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_reader_union():
    schema = {
        "type": "record",
        "name": "test_schema_migration_reader_union",
        "fields": [{"name": "test", "type": "int"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_reader_union",
        "fields": [{"name": "test", "type": ["string", "int"]}],
    }

    new_file = BytesIO()
    records = [{"test": 1}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_union_failure():
    schema = {
        "type": "record",
        "name": "test_schema_migration_union_failure",
        "fields": [{"name": "test", "type": "boolean"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_union_failure",
        "fields": [{"name": "test", "type": ["string", "int"]}],
    }

    new_file = BytesIO()
    records = [{"test": True}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)

    with pytest.raises(fastavro.read.SchemaResolutionError):
        list(new_reader)


def test_schema_migration_array_failure():
    schema = {
        "type": "record",
        "name": "test_schema_migration_array_failure",
        "fields": [
            {
                "name": "test",
                "type": {"type": "array", "items": ["string", "int"]},
            }
        ],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_array_failure",
        "fields": [
            {
                "name": "test",
                "type": {"type": "array", "items": ["string", "boolean"]},
            }
        ],
    }

    new_file = BytesIO()
    records = [{"test": [1, 2, 3]}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)

    with pytest.raises(fastavro.read.SchemaResolutionError):
        list(new_reader)


def test_schema_migration_maps_failure():
    schema = {
        "type": "record",
        "name": "test_schema_migration_maps_failure",
        "fields": [
            {
                "name": "test",
                "type": {"type": "map", "values": "string"},
            }
        ],
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_maps_failure",
        "fields": [
            {
                "name": "test",
                "type": {"type": "map", "values": "long"},
            }
        ],
    }

    new_file = BytesIO()
    records = [{"test": {"foo": "a"}}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    with pytest.raises(fastavro.read.SchemaResolutionError):
        list(new_reader)


def test_schema_migration_schema_mismatch():
    schema = {
        "type": "record",
        "name": "test_schema_migration_schema_mismatch",
        "fields": [
            {
                "name": "test",
                "type": "string",
            }
        ],
    }

    new_schema = {
        "type": "enum",
        "name": "test",
        "symbols": ["FOO", "BAR"],
    }

    new_file = BytesIO()
    records = [{"test": "test"}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    with pytest.raises(fastavro.read.SchemaResolutionError):
        list(new_reader)


def test_empty():
    io = BytesIO()
    with pytest.raises(ValueError, match="cannot read header - is it an avro file?"):
        fastavro.reader(io)


def test_no_default():
    io = BytesIO()
    schema = {
        "type": "record",
        "name": "test",
        "fields": [{"type": "boolean", "name": "a"}],
    }
    with pytest.raises(ValueError, match="no value and no default"):
        fastavro.writer(io, schema, [{}])


def test_is_avro_str():
    for path in iglob(f"{data_dir}/*.avro"):
        assert fastavro.is_avro(path)
    assert not fastavro.is_avro(__file__)


def test_is_avro_fo():
    for path in iglob(f"{data_dir}/*.avro"):
        with open(path, "rb") as fp:
            assert fastavro.is_avro(fp)
    with open(__file__, "rb") as fp:
        assert not fastavro.is_avro(fp)


def test_write_long_union_type():
    schema = {
        "name": "test_name",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "time", "type": ["null", "long"]},
        ],
    }

    records = [
        {"time": 809066167221092352},
    ]

    assert records == roundtrip(schema, records)


def test_cython_python():
    # Since Cython and Python implement the same behavior, it is possible for
    # build errors or coding errors to accidentally result in using the wrong
    # one. This is bad, because the pure Python version is faster in Pypy,
    # while the Cython version is faster in CPython. This test verifies the
    # correct reader and writer implementations are used.
    if hasattr(sys, "pypy_version_info"):
        # Pypy should not use Cython.
        assert not hasattr(_reader, "CYTHON_MODULE")
        assert not hasattr(_writer, "CYTHON_MODULE")
    else:
        # CPython should use Cython.
        assert getattr(_reader, "CYTHON_MODULE")
        assert getattr(_writer, "CYTHON_MODULE")


def test_writer_class_flush_end(tmpdir):
    """
    Create an Avro file using the Writer class. Verify that data accumulates in
    memory and is written when flush() is called.
    """
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {"name": "field1", "type": {"type": "string"}},
            {"name": "field2", "type": {"type": "int"}},
        ],
    }
    records = [{"field1": "test1", "field2": -1}, {"field1": "test2", "field2": 5}]

    temp_path = tmpdir.join("test_writer_class.avro")
    with temp_path.open("wb") as fo:
        w = Writer(fo, schema, codec="deflate")

        # Creating the Writer adds the Avro file header. Get file size with
        # header only.
        size_with_header_only = fo.tell()
        for i, record in enumerate(records):
            assert w.block_count == i
            w.write(record)

            # Verify records are being stored *in memory*:
            # 1. Block count increases
            # 2. File size does not increase
            assert w.block_count == i + 1
            assert fo.tell() == size_with_header_only

        # Flushing the file writes the data. File size should increase now.
        w.flush()
        assert fo.tell() > size_with_header_only

    # Read the records to verify they were written correctly.
    new_reader = fastavro.reader(temp_path.open("rb"))
    new_records = list(new_reader)
    assert new_records == records


def test_writer_class_sync_interval_automatic_flush(tmpdir):
    """
    Create an Avro file using the Writer class with sync_interval set to 0.
    Verify that data does not accumulate in memory but is automatically flushed
    to the file object as each record is added.
    """
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {"name": "field1", "type": {"type": "string"}},
            {"name": "field2", "type": {"type": "int"}},
        ],
    }
    records = [{"field1": "test1", "field2": -1}, {"field1": "test2", "field2": 5}]

    temp_path = tmpdir.join("test_writer_class.avro")
    with temp_path.open("wb") as fo:
        w = Writer(fo, schema, codec="deflate", sync_interval=0)

        # Creating the Writer adds the Avro file header. Get file size with
        # header only.
        file_size_history = [fo.tell()]
        for i, record in enumerate(records):
            assert w.block_count == 0
            w.write(record)

            # Verify records are being stored *in memory*:
            # 1. Block count increases
            # 2. File size does not increase
            assert w.block_count == 0
            file_size_history.append(fo.tell())
            assert file_size_history[-1] > file_size_history[-2]

        # Flushing the file writes the data. File size should increase now.
        w.flush()
        assert fo.tell() == file_size_history[-1]

    # Read the records to verify they were written correctly.
    new_reader = fastavro.reader(temp_path.open("rb"))
    new_records = list(new_reader)
    assert new_records == records


def test_writer_class_split_files(tmpdir):
    """
    Create 2 Avro files using the Writer class and the default sync_interval
    setting. We write to one file until the Writer automatically flushes, then
    write more records to the other file. Verify that the two files together
    contain all the records that were written.

    This simulates a real-world use case where a large Avro data set is split
    into files of approximately the same size.
    """
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{"name": "field", "type": {"type": "string"}}],
    }
    records = []

    def _append_record(writer_):
        record = {"field": f"test{len(records)}"}
        records.append(record)
        writer_.write(record)

    temp_paths = [
        tmpdir.join("test_writer_class1.avro"),
        tmpdir.join("test_writer_class2.avro"),
    ]
    interim_record_counts = []

    # First file: Write records until block_count goes back to 0 for the second
    # time.
    with temp_paths[0].open("wb") as fo:
        w = Writer(fo, schema, codec="deflate")
        _append_record(w)
        while w.block_count > 0:
            _append_record(w)
        _append_record(w)
        while w.block_count > 0:
            _append_record(w)
        w.flush()
    interim_record_counts.append(len(records))

    # Second file: 100 records
    with temp_paths[1].open("wb") as fo:
        w = Writer(fo, schema, codec="deflate")
        for i in range(100):
            _append_record(w)
        w.flush()
    interim_record_counts.append(len(records))

    assert interim_record_counts[1] == interim_record_counts[0] + 100

    # Read the records to verify they were written correctly.
    new_records = []
    new_interim_record_counts = []
    for temp_path in temp_paths:
        new_reader = fastavro.reader(temp_path.open("rb"))
        new_records += list(new_reader)
        new_interim_record_counts.append(len(new_records))
    assert new_records == records
    assert interim_record_counts == new_interim_record_counts


def test_union_records():
    #
    schema = {
        "name": "test_name",
        "namespace": "test",
        "type": "record",
        "fields": [
            {
                "name": "val",
                "type": [
                    {
                        "name": "a",
                        "namespace": "common",
                        "type": "record",
                        "fields": [
                            {"name": "x", "type": "int"},
                            {"name": "y", "type": "int"},
                        ],
                    },
                    {
                        "name": "b",
                        "namespace": "common",
                        "type": "record",
                        "fields": [
                            {"name": "x", "type": "int"},
                            {"name": "y", "type": "int"},
                            {"name": "z", "type": ["null", "int"]},
                        ],
                    },
                ],
            }
        ],
    }

    data = [
        {
            "val": {
                "x": 3,
                "y": 4,
                "z": 5,
            }
        }
    ]

    assert data == roundtrip(schema, data)


def test_ordered_dict_record():
    """
    Write an Avro record using an OrderedDict and read it back. This tests for
    a bug where dict was supported but not dict-like types.
    """
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{"name": "field", "type": {"type": "string"}}],
    }

    record = OrderedDict()
    record["field"] = "foobar"
    records = [record]

    assert records == roundtrip(schema, records)


def test_ordered_dict_map():
    """
    Write an Avro record containing a map field stored in an OrderedDict, then
    read it back. This tests for a bug where dict was supported but not
    dict-like types.
    """
    schema = {
        "type": "record",
        "name": "test_ordered_dict_map",
        "fields": [
            {
                "name": "test",
                "type": {"type": "map", "values": ["string", "int"]},
            }
        ],
    }

    map_ = OrderedDict()
    map_["foo"] = 1
    records = [{"test": map_}]

    assert records == roundtrip(schema, records)


@pytest.mark.skipif(
    not hasattr(_writer, "CYTHON_MODULE"), reason="Cython-specific test"
)
def test_regular_vs_ordered_dict_record_typeerror():
    """
    Tests a corner case where bad data in a dict record causes a TypeError. The
    specific failure lines in the backtrace should be different with dict vs
    OrderedDict, indicating the expected path was taken through the code.
    """
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{"name": "field", "type": {"type": "int"}}],
    }

    # Test with two different bad records. One is a regular dict, and the other
    # is an OrderedDict. Both have a bad value (string where the schema
    # declares an int).
    test_records = [{"field": "foobar"}]
    record = OrderedDict()
    record["field"] = "foobar"
    test_records.append(record)

    expected_write_record_stack_traces = [
        # For the regular dict, fails by reraising an error accessing
        # 'd_datum', a variable that only gets a value if the record is an
        # actual dict.
        ["write_data(fo, d_datum_value, field_type, named_schemas, name, options)"],
        # For the OrderedDict, fails directly when accessing 'datum', the
        # variable that is used if the record is *not* an actual dict.
        ["write_data(fo, datum_value, field_type, named_schemas, name, options)"],
    ]

    for test_record, expected_write_record_stack_trace in zip(
        test_records, expected_write_record_stack_traces
    ):
        new_file = BytesIO()
        records = [test_record]
        try:
            fastavro.writer(new_file, schema, records)
            assert False, "Should've raised TypeError"
        except TypeError:
            _, _, tb = sys.exc_info()
            stack = traceback.extract_tb(tb)
            filtered_stack = [frame[3] for frame in stack if "write_record" in frame[2]]
            assert filtered_stack == expected_write_record_stack_trace


@pytest.mark.skipif(
    not hasattr(_writer, "CYTHON_MODULE"), reason="Cython-specific test"
)
def test_regular_vs_ordered_dict_map_typeerror():
    """
    Tests a corner case where bad data in a dict map causes a TypeError. The
    specific failure lines in the backtrace should be different with dict vs
    OrderedDict, indicating the expected path was taken through the code.
    """
    schema = {
        "type": "record",
        "name": "test_regular_vs_ordered_dict_map_typeerror",
        "fields": [
            {
                "name": "test",
                "type": {"type": "map", "values": "int"},
            }
        ],
    }

    # Test with two different bad records. One is a regular dict, and the other
    # is an OrderedDict. Both have a bad value (string where the schema
    # declares an int).
    test_records = [{"test": {"foo": "bar"}}]
    map_ = OrderedDict()
    map_["foo"] = "bar"
    test_records.append({"test": map_})

    filtered_stacks = []
    for test_record in test_records:
        new_file = BytesIO()
        records = [test_record]
        try:
            fastavro.writer(new_file, schema, records)
            assert False, "Should've raised TypeError"
        except TypeError:
            _, _, tb = sys.exc_info()
            stack = traceback.extract_tb(tb)
            filtered_stack = [frame[1] for frame in stack if "write_map" in frame[2]]
            filtered_stacks.append(filtered_stack)

    # Because of the special-case code for dicts, the two stack traces should
    # be different, indicating the exception occurred at a different line
    # number.
    assert filtered_stacks[0] != filtered_stacks[1]


def test_write_union_tuple_primitive():
    """
    Test that when we can use tuple style of writing unions
    (see function `write_union` in `_write`) with primitives
     not only with records.
    """

    schema = {
        "name": "test_name",
        "namespace": "test",
        "type": "record",
        "fields": [{"name": "val", "type": ["string", "int"]}],
    }

    data = [
        {"val": ("int", 1)},
        {"val": ("string", "string")},
    ]

    expected_data = [
        {"val": 1},
        {"val": "string"},
    ]

    new_file = BytesIO()
    fastavro.writer(new_file, schema, data)
    new_file.seek(0)

    new_reader = fastavro.reader(new_file)
    new_records = list(new_reader)

    assert new_records == expected_data


def test_doubles_set_to_zero_on_windows():
    """https://github.com/fastavro/fastavro/issues/154"""

    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"},
            {"name": "test_float", "type": "double"},
        ],
    }

    records = [
        {
            "station": "011990-99999",
            "temp": 0,
            "test_float": 0.21334215134123513,
            "time": -714214260,
        },
        {
            "station": "011990-99999",
            "temp": 22,
            "test_float": 0.21334215134123513,
            "time": -714213259,
        },
        {
            "station": "011990-99999",
            "temp": -11,
            "test_float": 0.21334215134123513,
            "time": -714210269,
        },
        {
            "station": "012650-99999",
            "temp": 111,
            "test_float": 0.21334215134123513,
            "time": -714208170,
        },
    ]

    assert records == roundtrip(schema, records)


def test_string_not_treated_as_array():
    """https://github.com/fastavro/fastavro/issues/166"""

    schema = {
        "type": "record",
        "fields": [
            {
                "name": "description",
                "type": ["null", {"type": "array", "items": "string"}, "string"],
            }
        ],
        "name": "description",
        "doc": "A description of the thing.",
    }

    records = [
        {
            "description": "value",
        },
        {"description": ["an", "array"]},
    ]

    assert records == roundtrip(schema, records)


def test_schema_is_custom_dict_type():
    """https://github.com/fastavro/fastavro/issues/168"""

    class CustomDict(dict):
        pass

    schema = {
        "type": "record",
        "fields": [
            {
                "name": "description",
                "type": ["null", {"type": "array", "items": "string"}, "string"],
            }
        ],
        "name": "description",
        "doc": "A description of the thing.",
    }
    other_type_schema = CustomDict(schema)

    record = {
        "description": "value",
    }

    new_file = BytesIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, other_type_schema)
    assert record == new_record


def test_long_bounds():
    schema = {
        "name": "test_long_bounds",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "time", "type": "long"},
        ],
    }

    records = [
        {"time": (1 << 63) - 1},
        {"time": -(1 << 63)},
    ]

    assert records == roundtrip(schema, records)


def test_py37_runtime_error():
    """On Python 3.7 this test would cause the StopIteration to get raised as
    a RuntimeError.

    See https://www.python.org/dev/peps/pep-0479/
    """
    weather_file = join(data_dir, "weather.avro")

    zip_io = BytesIO()
    with zipfile.ZipFile(zip_io, mode="w") as zio:
        zio.write(weather_file, arcname="weather")

    with zipfile.ZipFile(zip_io) as zio:
        with zio.open("weather") as fo:
            # Need to read fo into a bytes buffer for python versions less
            # than 3.7
            reader = fastavro.reader(BytesIO(fo.read()))
            list(reader)


def test_eof_error():
    schema = {
        "type": "record",
        "name": "test_eof_error",
        "fields": [
            {
                "name": "test",
                "type": "float",
            }
        ],
    }

    new_file = BytesIO()
    record = {"test": 1.234}
    fastavro.schemaless_writer(new_file, schema, record)

    # Back up one byte and truncate
    new_file.seek(-1, 1)
    new_file.truncate()

    new_file.seek(0)
    with pytest.raises(EOFError):
        fastavro.schemaless_reader(new_file, schema)


def test_eof_error_string():
    schema = "string"
    new_file = BytesIO()
    fastavro.schemaless_writer(new_file, schema, "1234567890")

    # Back up one byte and truncate
    new_file.seek(-1, 1)
    new_file.truncate()

    new_file.seek(0)
    with pytest.raises(EOFError):
        fastavro.schemaless_reader(new_file, schema)


def test_eof_error_fixed():
    schema = {"type": "fixed", "size": 10, "name": "test"}
    new_file = BytesIO()
    fastavro.schemaless_writer(new_file, schema, b"1234567890")

    # Back up one byte and truncate
    new_file.seek(-1, 1)
    new_file.truncate()

    new_file.seek(0)
    with pytest.raises(EOFError):
        fastavro.schemaless_reader(new_file, schema)


def test_eof_error_bytes():
    schema = "bytes"
    new_file = BytesIO()
    fastavro.schemaless_writer(new_file, schema, b"1234567890")

    # Back up one byte and truncate
    new_file.seek(-1, 1)
    new_file.truncate()

    new_file.seek(0)
    with pytest.raises(EOFError):
        fastavro.schemaless_reader(new_file, schema)


def test_write_union_tuple_uses_namespaced_name():
    """
    Test that we must use the fully namespaced name when we are using the tuple
    style of writing unions

    https://github.com/fastavro/fastavro/issues/155
    """

    schema = {
        "name": "test_name",
        "namespace": "test",
        "type": "record",
        "fields": [
            {
                "name": "val",
                "type": [
                    {
                        "name": "A",
                        "namespace": "test",
                        "type": "record",
                        "fields": [
                            {"name": "field", "type": "int"},
                        ],
                    },
                    {
                        "name": "B",
                        "namespace": "test",
                        "type": "record",
                        "fields": [
                            {"name": "field", "type": "string"},
                        ],
                    },
                ],
            }
        ],
    }

    expected_data = [
        {"val": {"field": 1}},
        {"val": {"field": "string"}},
    ]

    data = [
        {"val": ("A", {"field": 1})},
        {"val": ("B", {"field": "string"})},
    ]

    # The given data doesn't use the namespaced name and should fail
    with pytest.raises(ValueError):
        assert expected_data == roundtrip(schema, data)

    data = [
        {"val": ("test.A", {"field": 1})},
        {"val": ("test.B", {"field": "string"})},
    ]

    # This passes because it uses the namespaced name
    assert expected_data == roundtrip(schema, data)


def test_passing_same_schema_to_reader():
    """https://github.com/fastavro/fastavro/issues/244"""
    schema = {
        "namespace": "test.avro.training",
        "name": "SomeMessage",
        "type": "record",
        "fields": [
            {
                "name": "is_error",
                "type": "boolean",
                "default": False,
            },
            {
                "name": "outcome",
                "type": [
                    "SomeMessage",
                    {
                        "type": "record",
                        "name": "ErrorRecord",
                        "fields": [
                            {
                                "name": "errors",
                                "type": {"type": "map", "values": "string"},
                                "doc": "doc",
                            }
                        ],
                    },
                ],
            },
        ],
    }

    records = [
        {
            "is_error": True,
            "outcome": {
                "errors": {"field_1": "some_message", "field_2": "some_other_message"}
            },
        }
    ]

    assert records == roundtrip(schema, records, reader_schema=schema)


def test_helpful_error_when_a_single_record_is_passed_to_writer():
    """https://github.com/fastavro/fastavro/issues/254"""
    schema = {
        "namespace": "namespace",
        "name": "name",
        "type": "record",
        "fields": [
            {
                "name": "is_error",
                "type": "boolean",
            }
        ],
    }

    record = {
        "is_error": True,
    }

    new_file = BytesIO()
    with pytest.raises(ValueError, match="argument should be an iterable, not dict"):
        fastavro.writer(new_file, schema, record)


def test_embedded_records_get_namespaced_correctly():
    schema = {
        "namespace": "test",
        "name": "OuterName",
        "type": "record",
        "fields": [
            {
                "name": "data",
                "type": [
                    {
                        "type": "record",
                        "name": "UUID",
                        "fields": [{"name": "uuid", "type": "string"}],
                    },
                    {
                        "type": "record",
                        "name": "Abstract",
                        "fields": [
                            {
                                "name": "uuid",
                                "type": "UUID",
                            }
                        ],
                    },
                    {
                        "type": "record",
                        "name": "Concrete",
                        "fields": [
                            {"name": "abstract", "type": "Abstract"},
                            {
                                "name": "custom",
                                "type": "string",
                            },
                        ],
                    },
                ],
            }
        ],
    }

    records = [
        {"data": {"abstract": {"uuid": {"uuid": "some_uuid"}}, "custom": "some_string"}}
    ]

    assert records == roundtrip(schema, records)


def test_null_defaults_are_not_used():
    """https://github.com/fastavro/fastavro/issues/272"""
    schema = [
        {
            "type": "record",
            "name": "A",
            "fields": [{"name": "foo", "type": ["string", "null"]}],
        },
        {
            "type": "record",
            "name": "B",
            "fields": [{"name": "bar", "type": ["string", "null"]}],
        },
        {
            "type": "record",
            "name": "AOrB",
            "fields": [{"name": "entity", "type": ["A", "B"]}],
        },
    ]

    datum_to_read = {"entity": {"foo": "this is an instance of schema A"}}

    assert [datum_to_read] == roundtrip(schema, [datum_to_read])


def test_union_schema_ignores_extra_fields():
    """https://github.com/fastavro/fastavro/issues/274"""
    schema = [
        {"type": "record", "name": "A", "fields": [{"name": "name", "type": "string"}]},
        {
            "type": "record",
            "name": "B",
            "fields": [{"name": "other_name", "type": "string"}],
        },
    ]

    records = [{"name": "abc", "other": "asd"}]

    assert [{"name": "abc"}] == roundtrip(schema, records)


def test_appending_records(tmpdir):
    """https://github.com/fastavro/fastavro/issues/276"""
    schema = {
        "type": "record",
        "name": "test_appending_records",
        "fields": [
            {
                "name": "field",
                "type": "string",
            }
        ],
    }

    test_file = str(tmpdir.join("test.avro"))

    with open(test_file, "wb") as new_file:
        fastavro.writer(new_file, schema, [{"field": "foo"}])

    with open(test_file, "a+b") as new_file:
        fastavro.writer(new_file, schema, [{"field": "bar"}])

    with open(test_file, "rb") as new_file:
        reader = fastavro.reader(new_file)
        new_records = list(reader)

    assert new_records == [{"field": "foo"}, {"field": "bar"}]


def test_appending_records_with_io_stream():
    """https://github.com/fastavro/fastavro/issues/276"""
    schema = {
        "type": "record",
        "name": "test_appending_records_with_io_stream",
        "fields": [
            {
                "name": "field",
                "type": "string",
            }
        ],
    }

    stream = BytesIO()

    fastavro.writer(stream, schema, [{"field": "foo"}])

    # Should be able to append to the existing stream
    fastavro.writer(stream, schema, [{"field": "bar"}])

    stream.seek(0)
    reader = fastavro.reader(stream)
    new_records = list(reader)

    assert new_records == [{"field": "foo"}, {"field": "bar"}]

    # If we seek to the beginning and write, it will be treated like a brand
    # new file
    stream.seek(0)
    fastavro.writer(stream, schema, [{"field": "abcdefghijklmnopqrstuvwxyz"}])

    stream.seek(0)
    reader = fastavro.reader(stream)
    new_records = list(reader)

    assert new_records == [{"field": "abcdefghijklmnopqrstuvwxyz"}]


def test_appending_records_wrong_mode_fails(tmpdir):
    """https://github.com/fastavro/fastavro/issues/276"""
    schema = {
        "type": "record",
        "name": "test_appending_records_wrong_mode_fails",
        "fields": [
            {
                "name": "field",
                "type": "string",
            }
        ],
    }

    test_file = str(tmpdir.join("test.avro"))

    with open(test_file, "wb") as new_file:
        fastavro.writer(new_file, schema, [{"field": "foo"}])

    with open(test_file, "ab") as new_file:
        with pytest.raises(
            ValueError, match=r"you must use the 'a\+' mode, not just 'a'"
        ):
            fastavro.writer(new_file, schema, [{"field": "bar"}])


def test_appending_records_different_schema_works(tmpdir):
    """https://github.com/fastavro/fastavro/issues/276"""
    schema = {
        "type": "record",
        "name": "test_appending_records_different_schema_fails",
        "fields": [{"name": "field", "type": "string"}],
    }

    test_file = str(tmpdir.join("test.avro"))

    with open(test_file, "wb") as new_file:
        fastavro.writer(new_file, schema, [{"field": "foo"}])

    different_schema = {
        "type": "record",
        "name": "test_appending_records",
        "fields": [{"name": "field", "type": "int"}],
    }

    with open(test_file, "a+b") as new_file:
        fastavro.writer(new_file, different_schema, [{"field": "bar"}])


def test_appending_records_different_schema_works_2(tmpdir):
    """https://github.com/fastavro/fastavro/issues/276"""
    schema = {
        "type": "record",
        "name": "test_appending_records_different_schema_fails",
        "fields": [
            {"name": "field", "type": "string"},
            {
                "name": "field2",
                "type": {
                    "type": "record",
                    "name": "subrecord",
                    "fields": [{"name": "subfield", "type": "string"}],
                },
            },
            {"name": "field3", "type": "subrecord"},
        ],
    }

    test_file = str(tmpdir.join("test.avro"))

    with open(test_file, "wb") as new_file:
        fastavro.writer(
            new_file,
            schema,
            [
                {
                    "field": "foo",
                    "field2": {"subfield": "foo2"},
                    "field3": {"subfield": "foo3"},
                }
            ],
        )

    different_schema = {
        "type": "record",
        "name": "test_appending_records",
        "fields": [{"name": "field", "type": "int"}],
    }

    with open(test_file, "a+b") as new_file:
        fastavro.writer(
            new_file,
            different_schema,
            [
                {
                    "field": "bar",
                    "field2": {"subfield": "bar2"},
                    "field3": {"subfield": "bar3"},
                }
            ],
        )


def test_appending_records_null_schema_works(tmpdir):
    """https://github.com/fastavro/fastavro/issues/422"""
    schema = {
        "type": "record",
        "name": "test_appending_records_different_schema_fails",
        "fields": [{"name": "field", "type": "string"}],
    }

    test_file = str(tmpdir.join("test.avro"))

    with open(test_file, "wb") as new_file:
        fastavro.writer(new_file, schema, [{"field": "foo"}])

    with open(test_file, "a+b") as new_file:
        fastavro.writer(new_file, None, [{"field": "bar"}])


def test_user_specified_sync():
    """https://github.com/fastavro/fastavro/issues/300"""
    schema = {"type": "record", "name": "test_user_specified_sync", "fields": []}

    file1 = BytesIO()
    file2 = BytesIO()

    records = [{}]

    fastavro.writer(file1, schema, records, sync_marker=b"16bytesyncmarker")
    fastavro.writer(file2, schema, records, sync_marker=b"16bytesyncmarker")

    assert file1.getvalue() == file2.getvalue()


def test_order_of_values_in_map():
    """https://github.com/fastavro/fastavro/issues/303"""
    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {
                "name": "metadata",
                "type": {
                    "type": "map",
                    "values": [
                        {"type": "array", "items": "string"},
                        {"type": "map", "values": ["string"]},
                    ],
                },
            }
        ],
    }
    parsed_schema = fastavro.parse_schema(schema)

    records = [{"metadata": {"map1": {"map2": "str"}}}]

    assert records == roundtrip(parsed_schema, records)


def test_reader_schema_attributes_throws_deprecation():
    """https://github.com/fastavro/fastavro/issues/246"""
    schema = {
        "type": "record",
        "name": "test_reader_schema_attributes_throws_deprecation",
        "fields": [],
    }

    stream = BytesIO()

    fastavro.writer(stream, schema, [{}])
    stream.seek(0)

    reader = fastavro.reader(stream)
    with pytest.warns(DeprecationWarning):
        reader.schema


def test_writer_schema_always_read():
    """https://github.com/fastavro/fastavro/issues/312"""
    schema = {
        "type": "record",
        "name": "Outer",
        "fields": [
            {
                "name": "item",
                "type": [
                    {
                        "type": "record",
                        "name": "Inner1",
                        "fields": [
                            {
                                "name": "id",
                                "type": {
                                    "type": "record",
                                    "name": "UUID",
                                    "fields": [{"name": "id", "type": "string"}],
                                },
                                "default": {"id": ""},
                            },
                            {"name": "description", "type": "string"},
                            {"name": "size", "type": "int"},
                        ],
                    },
                    {
                        "type": "record",
                        "name": "Inner2",
                        "fields": [
                            {"name": "id", "type": "UUID", "default": {"id": ""}},
                            {"name": "name", "type": "string"},
                            {"name": "age", "type": "long"},
                        ],
                    },
                ],
            }
        ],
    }

    records = [
        {"item": {"description": "test", "size": 1}},
        {"item": {"id": {"id": "#1"}, "name": "foobar", "age": 12}},
    ]

    file = BytesIO()

    fastavro.writer(file, fastavro.parse_schema(schema), records)
    file.seek(0)

    # This should not raise a KeyError
    fastavro.reader(file)


def test_hint_is_not_written_to_the_file():
    """The __fastavro_parsed hint should not be written to the avrofile"""
    schema = {
        "type": "record",
        "name": "test_hint_is_not_written_to_the_file",
        "fields": [],
    }

    parsed_schema = fastavro.parse_schema(schema)

    # It should get added when parsing
    assert "__fastavro_parsed" in parsed_schema

    stream = BytesIO()
    fastavro.writer(stream, parsed_schema, [{}])
    stream.seek(0)

    reader = fastavro.reader(stream)
    # By the time it is in the file, it should not
    assert "__fastavro_parsed" not in reader._schema


def test_hint_is_not_written_to_the_file_list_schema():
    """The __fastavro_parsed hint should not be written to the avrofile"""
    schema = [
        {
            "type": "record",
            "name": "test_hint_is_not_written_to_the_file_list_schema_1",
            "fields": [],
        },
        {
            "type": "record",
            "name": "test_hint_is_not_written_to_the_file_list_schema_2",
            "fields": [],
        },
    ]

    parsed_schema = fastavro.parse_schema(schema)

    # It should get added when parsing
    assert all(("__fastavro_parsed" in s for s in parsed_schema))

    stream = BytesIO()
    fastavro.writer(stream, parsed_schema, [{}])
    stream.seek(0)

    reader = fastavro.reader(stream)
    # By the time it is in the file, it should not
    assert all(("__fastavro_parsed" not in s for s in reader._schema))


def test_more_null_union_issues():
    """https://github.com/fastavro/fastavro/issues/336"""
    schema = {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {
                "name": "address",
                "type": [
                    "null",
                    {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "whatever",
                            "fields": [
                                {"name": "street", "type": ["null", "string"]},
                                {"name": "zip", "type": ["null", "string"]},
                            ],
                        },
                    },
                ],
            },
        ],
    }

    records = [
        {"name": "name1", "address": [{"street": "22st"}]},
        {"name": "name2"},
    ]

    expected = [
        {"name": "name1", "address": [{"street": "22st", "zip": None}]},
        {"name": "name2", "address": None},
    ]

    assert expected == roundtrip(schema, records)


def test_logical_type_in_union():
    schema = {
        "type": "record",
        "name": "test_logical_type_in_union",
        "fields": [
            {"name": "item", "type": ["null", {"type": "int", "logicalType": "date"}]}
        ],
    }

    records = [{"item": None}, {"item": "2019-05-06"}]

    expected = [{"item": None}, {"item": datetime.date(2019, 5, 6)}]

    assert expected == roundtrip(schema, records)


def test_named_schema_with_logical_type_in_union():
    schema = [
        {
            "name": "named_schema_with_logical_type",
            "namespace": "com.example",
            "type": "record",
            "fields": [
                {"name": "item", "type": {"type": "int", "logicalType": "date"}}
            ],
        },
        {
            "type": "record",
            "name": "test_named_schema_with_logical_type",
            "fields": [
                {
                    "name": "item",
                    "type": ["null", "com.example.named_schema_with_logical_type"],
                }
            ],
        },
    ]

    records = [{"item": None}, {"item": {"item": "2019-05-06"}}]

    expected = [{"item": None}, {"item": {"item": datetime.date(2019, 5, 6)}}]

    assert expected == roundtrip(schema, records)


def test_return_record_name_with_named_type_in_union():
    schema = {
        "type": "record",
        "name": "my_record",
        "fields": [
            {
                "name": "my_1st_union",
                "type": [
                    {
                        "name": "foo",
                        "type": "record",
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                    {
                        "name": "bar",
                        "type": "record",
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                ],
            },
            {"name": "my_2nd_union", "type": ["foo", "bar"]},
        ],
    }

    records = [
        {
            "my_1st_union": ("foo", {"some_field": 1}),
            "my_2nd_union": ("bar", {"some_field": 2}),
        }
    ]

    rt_records = roundtrip(
        fastavro.parse_schema(schema), records, return_record_name=True
    )
    assert records == rt_records


def test_return_record_with_named_type_in_union():
    """https://github.com/fastavro/fastavro/issues/625"""
    schema = {
        "type": "record",
        "name": "my_record",
        "fields": [
            {
                "name": "my_union",
                "type": [
                    "null",
                    {
                        "name": "bar",
                        "type": "record",
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                ],
            }
        ],
    }

    records = [{"my_union": None}, {"my_union": {"some_field": 2}}]

    rt_records = roundtrip(
        fastavro.parse_schema(schema),
        records,
        return_record_name=True,
        return_record_name_override=True,
    )
    assert records == rt_records


def test_return_record_name_with_named_type_and_null_in_union():
    """https://github.com/fastavro/fastavro/issues/625"""
    schema = {
        "type": "record",
        "name": "my_record",
        "fields": [
            {
                "name": "my_union",
                "type": [
                    "null",
                    {
                        "name": "foo",
                        "type": "record",
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                    {
                        "name": "bar",
                        "type": "record",
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                ],
            }
        ],
    }

    records = [
        {"my_union": None},
        {"my_union": ("foo", {"some_field": 1})},
        {"my_union": ("bar", {"some_field": 2})},
    ]

    rt_records = roundtrip(
        fastavro.parse_schema(schema),
        records,
        return_record_name=True,
        return_record_name_override=True,
    )
    assert records == rt_records


def test_return_record_name_with_named_type_and_null_in_union2():
    """https://github.com/fastavro/fastavro/issues/625"""
    schema = {
        "type": "record",
        "name": "my_record",
        "fields": [
            {
                "name": "foo",
                "type": {
                    "type": "record",
                    "name": "Foo",
                    "fields": [{"name": "subfoo", "type": "string"}],
                },
            },
            {
                "name": "my_union",
                "type": [
                    "null",
                    "Foo",
                    {
                        "name": "bar",
                        "type": "record",
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                ],
            },
        ],
    }

    records = [
        {"foo": {"subfoo": "subfoo"}, "my_union": None},
        {"foo": {"subfoo": "subfoo"}, "my_union": ("Foo", {"subfoo": "1"})},
        {"foo": {"subfoo": "subfoo"}, "my_union": ("bar", {"some_field": 2})},
    ]

    rt_records = roundtrip(
        fastavro.parse_schema(schema),
        records,
        return_record_name=True,
        return_record_name_override=True,
    )
    assert records == rt_records


def test_return_record_with_multiple_simple_types_and_null_in_union():
    """https://github.com/fastavro/fastavro/issues/625"""
    schema = {
        "type": "record",
        "name": "my_record",
        "fields": [
            {
                "name": "my_union",
                "type": [
                    "null",
                    "string",
                    "int",
                    {
                        "name": "foo",
                        "type": "record",
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                ],
            }
        ],
    }

    records = [
        {"my_union": None},
        {"my_union": "3"},
        {"my_union": 3},
        {"my_union": {"some_field": 2}},
    ]

    rt_records = roundtrip(
        fastavro.parse_schema(schema),
        records,
        return_record_name=True,
        return_record_name_override=True,
    )
    assert records == rt_records


def test_return_record_with_multiple_dict_types_and_null_in_union():
    schema = {
        "type": "record",
        "name": "my_record",
        "fields": [
            {
                "name": "my_union",
                "type": [
                    "null",
                    "int",
                    {
                        "type": "enum",
                        "name": "my_enum",
                        "symbols": ["FOO", "BAR"],
                    },
                    {
                        "name": "foo",
                        "type": "record",
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                ],
            }
        ],
    }

    records = [
        {"my_union": None},
        {"my_union": 3},
        {"my_union": "FOO"},
        {"my_union": {"some_field": 2}},
    ]

    rt_records = roundtrip(
        fastavro.parse_schema(schema),
        records,
        return_record_name=True,
        return_record_name_override=True,
    )
    assert records == rt_records


def test_enum_named_type():
    """https://github.com/fastavro/fastavro/issues/450"""
    schema = {
        "type": "record",
        "name": "test_enum_named_type",
        "fields": [
            {
                "name": "test1",
                "type": {
                    "type": "enum",
                    "name": "my_enum",
                    "symbols": ["FOO", "BAR"],
                },
            },
            {
                "name": "test2",
                "type": "my_enum",
            },
        ],
    }

    records = [{"test1": "FOO", "test2": "BAR"}]
    parsed_schema = fastavro.parse_schema(schema)
    assert records == roundtrip(parsed_schema, records)


def test_fixed_named_type():
    """https://github.com/fastavro/fastavro/issues/450"""
    schema = {
        "type": "record",
        "name": "test_fixed_named_type",
        "fields": [
            {
                "name": "test1",
                "type": {
                    "type": "fixed",
                    "name": "my_fixed",
                    "size": 4,
                },
            },
            {
                "name": "test2",
                "type": "my_fixed",
            },
        ],
    }

    records = [{"test1": b"1234", "test2": b"4321"}]
    parsed_schema = fastavro.parse_schema(schema)
    assert records == roundtrip(parsed_schema, records)


def test_record_named_type():
    """https://github.com/fastavro/fastavro/issues/450"""
    schema = {
        "type": "record",
        "name": "test_record_named_type",
        "fields": [
            {
                "name": "test1",
                "type": {
                    "type": "record",
                    "name": "my_record",
                    "fields": [
                        {
                            "name": "field1",
                            "type": "string",
                        }
                    ],
                },
            },
            {
                "name": "test2",
                "type": "my_record",
            },
        ],
    }

    records = [{"test1": {"field1": "foo"}, "test2": {"field1": "bar"}}]
    parsed_schema = fastavro.parse_schema(schema)
    assert records == roundtrip(parsed_schema, records)


def test_write_required_field_name():
    """https://github.com/fastavro/fastavro/issues/439

    Test that when a TypeError is raised the fieldname is
    included in exception message allowing to figure out
    quickly what column value mismatches the schema field type.
    Useful for Null/None values in required fields also.
    """

    schema = {
        "name": "test_required",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "person", "type": "string"},
            {"name": "age", "type": "int"},
        ],
    }

    data = [
        {"person": "James", "age": 34},
        {"person": "Anthony", "age": None},
    ]

    new_file = BytesIO()
    with pytest.raises(TypeError, match="on field age"):
        fastavro.writer(new_file, schema, data)


def test_write_mismatched_field_type():
    """https://github.com/fastavro/fastavro/issues/439

    Test that when a ValueError is raised the fieldname is
    included in exception message allowing to figure out
    quickly what column mismatches the schema field type.
    """

    schema = {
        "name": "test_required",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "person", "type": "string"},
            {"name": "age", "type": ["null", "int"]},
        ],
    }

    data = [
        {"person": "James", "age": 34},
        {"person": "Anthony", "age": "26"},
    ]

    new_file = BytesIO()
    with pytest.raises(ValueError, match="on field age"):
        fastavro.writer(new_file, schema, data)


def test_reading_with_subschema():
    """https://github.com/fastavro/fastavro/issues/503"""
    writer_schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {"name": "null", "type": "null"},
            {"name": "boolean", "type": "boolean"},
            {"name": "string", "type": "string"},
            {"name": "bytes", "type": "bytes"},
            {"name": "int", "type": "int"},
            {"name": "long", "type": "long"},
            {"name": "float", "type": "float"},
            {"name": "double", "type": "double"},
            {
                "name": "fixed",
                "type": {"type": "fixed", "name": "fixed_field", "size": 5},
            },
            {
                "name": "union",
                "type": [
                    "null",
                    "int",
                    {
                        "type": "record",
                        "name": "union_record",
                        "fields": [{"name": "union_record_field", "type": "string"}],
                    },
                ],
            },
            {
                "name": "enum",
                "type": {
                    "type": "enum",
                    "name": "enum_field",
                    "symbols": ["FOO", "BAR"],
                },
            },
            {"name": "array", "type": {"type": "array", "items": "string"}},
            {"name": "map", "type": {"type": "map", "values": "int"}},
            {
                "name": "record",
                "type": {
                    "type": "record",
                    "name": "subrecord",
                    "fields": [{"name": "sub_int", "type": "int"}],
                },
            },
        ],
    }

    reader_schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {"name": "string", "type": "string"},
            {"name": "double", "type": "double"},
            {
                "name": "enum",
                "type": {
                    "type": "enum",
                    "name": "enum_field",
                    "symbols": ["FOO", "BAR"],
                },
            },
        ],
    }

    records = [
        {
            "null": None,
            "boolean": True,
            "string": "foo",
            "bytes": b"\xe2\x99\xa5",
            "int": 1,
            "long": 1 << 33,
            "float": 2.2,
            "double": 3.3,
            "fixed": b"\x61\x61\x61\x61\x61",
            "union": None,
            "enum": "BAR",
            "array": ["a", "b"],
            "map": {"c": 1, "d": 2},
            "record": {
                "sub_int": 123,
            },
        },
    ]

    roundtrip_records = roundtrip(writer_schema, records, reader_schema=reader_schema)
    assert roundtrip_records == [{"string": "foo", "double": 3.3, "enum": "BAR"}]


@pytest.mark.skipif(
    not hasattr(_reader, "CYTHON_MODULE"), reason="Only works using cython module"
)
def test_reading_with_skip_using_cython():
    """https://github.com/fastavro/fastavro/issues/503"""
    writer_schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {"name": "null", "type": "null"},
            {"name": "boolean", "type": "boolean"},
            {"name": "string", "type": "string"},
            {"name": "bytes", "type": "bytes"},
            {"name": "int", "type": "int"},
            {"name": "long", "type": "long"},
            {"name": "float", "type": "float"},
            {"name": "double", "type": "double"},
            {
                "name": "fixed",
                "type": {"type": "fixed", "name": "fixed_field", "size": 5},
            },
            {
                "name": "union",
                "type": [
                    "null",
                    "int",
                    {
                        "type": "record",
                        "name": "union_record",
                        "fields": [{"name": "union_record_field", "type": "string"}],
                    },
                ],
            },
            {
                "name": "enum",
                "type": {
                    "type": "enum",
                    "name": "enum_field",
                    "symbols": ["FOO", "BAR"],
                },
            },
            {
                "name": "enum2",
                "type": {
                    "type": "enum",
                    "name": "enum_field2",
                    "symbols": ["BAZ", "BAZBAZ"],
                },
            },
            {"name": "array", "type": {"type": "array", "items": "string"}},
            {"name": "map", "type": {"type": "map", "values": "int"}},
            {
                "name": "record",
                "type": {
                    "type": "record",
                    "name": "subrecord",
                    "fields": [{"name": "sub_int", "type": "int"}],
                },
            },
            {"name": "record2", "type": "test.subrecord"},
        ],
    }

    reader_schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {"name": "string", "type": "string"},
            {"name": "double", "type": "double"},
            {
                "name": "enum",
                "type": {
                    "type": "enum",
                    "name": "enum_field",
                    "symbols": ["FOO", "BAR"],
                },
            },
        ],
    }

    records = [
        {
            "null": None,
            "boolean": True,
            "string": "foo",
            "bytes": b"\xe2\x99\xa5",
            "int": 1,
            "long": 1 << 33,
            "float": 2.2,
            "double": 3.3,
            "fixed": b"\x61\x61\x61\x61\x61",
            "union": None,
            "enum": "BAR",
            "enum2": "BAZBAZ",
            "array": ["a", "b"],
            "map": {"c": 1, "d": 2},
            "record": {"sub_int": 123},
            "record2": {"sub_int": 321},
        },
    ]

    named_schemas = {}
    parsed_writer_schema = fastavro.parse_schema(writer_schema, named_schemas)
    roundtrip_records = roundtrip(
        parsed_writer_schema, records, reader_schema=reader_schema
    )

    new_file = BytesIO()
    fastavro.writer(new_file, parsed_writer_schema, records)
    new_file.seek(0)

    skip_record = {}
    _reader.skip_record(new_file, HEADER_SCHEMA, {})

    block_count = _reader.read_long(new_file)
    assert block_count == 1

    # Skip size in bytes of the serialized objects in the block
    _reader.skip_long(new_file)

    _reader.skip_null(new_file)
    _reader.skip_boolean(new_file)
    skip_record["string"] = _reader.read_utf8(new_file)
    _reader.skip_bytes(new_file)
    _reader.skip_int(new_file)
    _reader.skip_long(new_file)
    _reader.skip_float(new_file)
    skip_record["double"] = _reader.read_double(new_file)
    _reader.skip_fixed(new_file, {"type": "fixed", "name": "fixed_field", "size": 5})
    _reader.skip_union(
        new_file,
        [
            "null",
            "int",
            {
                "type": "record",
                "name": "union_record",
                "fields": [{"name": "union_record_field", "type": "string"}],
            },
        ],
        named_schemas,
    )
    skip_record["enum"] = _reader.read_enum(
        new_file,
        {"type": "enum", "name": "enum_field", "symbols": ["FOO", "BAR"]},
        {"type": "enum", "name": "enum_field", "symbols": ["FOO", "BAR"]},
    )
    _reader.skip_enum(new_file)
    _reader.skip_array(new_file, {"type": "array", "items": "string"}, named_schemas)
    _reader.skip_map(new_file, {"type": "map", "values": "int"}, named_schemas)
    _reader.skip_record(
        new_file,
        {
            "type": "record",
            "name": "subrecord",
            "fields": [{"name": "sub_int", "type": "int"}],
        },
        named_schemas,
    )
    _reader.skip_record(
        new_file,
        {
            "type": "record",
            "name": "subrecord",
            "fields": [{"name": "sub_int", "type": "int"}],
        },
        named_schemas,
    )

    assert roundtrip_records == [skip_record]


@pytest.mark.skipif(
    hasattr(_reader, "CYTHON_MODULE"), reason="Only works when not using cython module"
)
def test_reading_with_skip_using_pure_python():
    """https://github.com/fastavro/fastavro/issues/503"""
    writer_schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {"name": "null", "type": "null"},
            {"name": "boolean", "type": "boolean"},
            {"name": "string", "type": "string"},
            {"name": "bytes", "type": "bytes"},
            {"name": "int", "type": "int"},
            {"name": "long", "type": "long"},
            {"name": "float", "type": "float"},
            {"name": "double", "type": "double"},
            {
                "name": "fixed",
                "type": {"type": "fixed", "name": "fixed_field", "size": 5},
            },
            {
                "name": "union",
                "type": [
                    "null",
                    "int",
                    {
                        "type": "record",
                        "name": "union_record",
                        "fields": [{"name": "union_record_field", "type": "string"}],
                    },
                ],
            },
            {
                "name": "enum",
                "type": {
                    "type": "enum",
                    "name": "enum_field",
                    "symbols": ["FOO", "BAR"],
                },
            },
            {
                "name": "enum2",
                "type": {
                    "type": "enum",
                    "name": "enum_field2",
                    "symbols": ["BAZ", "BAZBAZ"],
                },
            },
            {"name": "array", "type": {"type": "array", "items": "string"}},
            {"name": "map", "type": {"type": "map", "values": "int"}},
            {
                "name": "record",
                "type": {
                    "type": "record",
                    "name": "subrecord",
                    "fields": [{"name": "sub_int", "type": "int"}],
                },
            },
            {"name": "record2", "type": "test.subrecord"},
        ],
    }

    reader_schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {"name": "string", "type": "string"},
            {"name": "double", "type": "double"},
            {
                "name": "enum",
                "type": {
                    "type": "enum",
                    "name": "enum_field",
                    "symbols": ["FOO", "BAR"],
                },
            },
        ],
    }

    records = [
        {
            "null": None,
            "boolean": True,
            "string": "foo",
            "bytes": b"\xe2\x99\xa5",
            "int": 1,
            "long": 1 << 33,
            "float": 2.2,
            "double": 3.3,
            "fixed": b"\x61\x61\x61\x61\x61",
            "union": None,
            "enum": "BAR",
            "enum2": "BAZBAZ",
            "array": ["a", "b"],
            "map": {"c": 1, "d": 2},
            "record": {"sub_int": 123},
            "record2": {"sub_int": 321},
        },
    ]

    named_schemas = {}
    parsed_writer_schema = fastavro.parse_schema(writer_schema, named_schemas)
    roundtrip_records = roundtrip(
        parsed_writer_schema, records, reader_schema=reader_schema
    )

    new_file = BytesIO()
    fastavro.writer(new_file, parsed_writer_schema, records)
    new_file.seek(0)

    skip_record = {}
    decoder = BinaryDecoder(new_file)
    _reader.skip_record(decoder, HEADER_SCHEMA, {})

    block_count = _reader.read_long(decoder)
    assert block_count == 1

    # Skip size in bytes of the serialized objects in the block
    _reader.skip_long(decoder)

    _reader.skip_null(decoder)
    _reader.skip_boolean(decoder)
    skip_record["string"] = _reader.read_utf8(decoder)
    _reader.skip_bytes(decoder)
    _reader.skip_int(decoder)
    _reader.skip_long(decoder)
    _reader.skip_float(decoder)
    skip_record["double"] = _reader.read_double(decoder)
    _reader.skip_fixed(decoder, {"type": "fixed", "name": "fixed_field", "size": 5})
    _reader.skip_union(
        decoder,
        [
            "null",
            "int",
            {
                "type": "record",
                "name": "union_record",
                "fields": [{"name": "union_record_field", "type": "string"}],
            },
        ],
        named_schemas,
    )
    skip_record["enum"] = _reader.read_enum(
        decoder,
        {"type": "enum", "name": "enum_field", "symbols": ["FOO", "BAR"]},
        named_schemas,
        {"type": "enum", "name": "enum_field", "symbols": ["FOO", "BAR"]},
    )
    _reader.skip_enum(
        decoder,
        {"type": "enum", "name": "enum_field2", "symbols": ["BAZ", "BAZBAZ"]},
        {"type": "enum", "name": "enum_field2", "symbols": ["BAZ", "BAZBAZ"]},
    )
    _reader.skip_array(decoder, {"type": "array", "items": "string"}, named_schemas)
    _reader.skip_map(decoder, {"type": "map", "values": "int"}, named_schemas)
    _reader.skip_record(
        decoder,
        {
            "type": "record",
            "name": "subrecord",
            "fields": [{"name": "sub_int", "type": "int"}],
        },
        named_schemas,
    )
    _reader.skip_record(
        decoder,
        {
            "type": "record",
            "name": "subrecord",
            "fields": [{"name": "sub_int", "type": "int"}],
        },
        named_schemas,
    )

    assert roundtrip_records == [skip_record]


def test_tuple_writer_picks_correct_union_path():
    """https://github.com/fastavro/fastavro/issues/509"""
    schema = {
        "type": "record",
        "name": "test_tuple_writer_picks_correct_union_path",
        "fields": [
            {
                "name": "Field",
                "type": [
                    {"type": "map", "values": "string"},
                    {
                        "type": "record",
                        "name": "Record2",
                        "fields": [{"name": "Field", "type": "string"}],
                    },
                ],
            },
        ],
    }

    records = [{"Field": ("Record2", {"Field": "value"})}]
    parsed_schema = fastavro.parse_schema(schema)
    assert records == roundtrip(parsed_schema, records, return_record_name=True)

    records = [{"Field": ("map", {"Field": "value"})}]
    expected_roundtrip_value = [{"Field": {"Field": "value"}}]
    parsed_schema = fastavro.parse_schema(schema)
    assert expected_roundtrip_value == roundtrip(
        parsed_schema, records, return_record_name=True
    )


def test_schema_migration_should_match_name():
    """https://github.com/fastavro/fastavro/issues/512"""
    writer_schema = {
        "type": "record",
        "name": "root",
        "namespace": "example.space",
        "fields": [
            {"name": "id", "type": "string"},
            {
                "name": "data",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "SomeData",
                        "fields": [
                            {"name": "somedata_id", "type": "int"},
                            {"name": "somedata_field", "type": "int"},
                        ],
                    },
                    {
                        "type": "record",
                        "name": "OtherData",
                        "fields": [
                            {"name": "otherdata_id", "type": "int"},
                            {"name": "otherdata_field", "type": "string"},
                        ],
                    },
                ],
            },
        ],
    }

    reader_schema = {
        "type": "record",
        "name": "root",
        "namespace": "example.space",
        "fields": [
            {"name": "id", "type": "string"},
            {
                "name": "data",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "SomeData",
                        "fields": [
                            {"name": "somedata_id", "type": "int"},
                            {"name": "somedata_field", "type": "int"},
                        ],
                    },
                    {
                        "type": "record",
                        "name": "OtherData",
                        "fields": [
                            {"name": "otherdata_id", "type": "int"},
                            {"name": "otherdata_field", "type": "string"},
                            # Fully compatible change
                            {
                                "name": "newdata_field",
                                "type": "string",
                                "default": "new stuff",
                            },
                        ],
                    },
                ],
            },
        ],
    }

    records = [
        {
            "id": "example_id_1234",
            "data": {"otherdata_id": 1234, "otherdata_field": "some example data"},
        }
    ]

    roundtrip_records = roundtrip(writer_schema, records, reader_schema=reader_schema)

    expected_roundtrip = [
        {
            "id": "example_id_1234",
            "data": {
                "otherdata_id": 1234,
                "otherdata_field": "some example data",
                "newdata_field": "new stuff",
            },
        }
    ]
    assert roundtrip_records == expected_roundtrip


def test_schema_migration_should_fail_with_different_names():
    """https://github.com/fastavro/fastavro/issues/515"""
    writer_schema = {
        "type": "record",
        "name": "A",
        "fields": [{"type": "int", "name": "F"}],
    }

    reader_schema = {
        "type": "record",
        "name": "B",
        "fields": [{"type": "int", "name": "F"}],
    }

    records = [{"F": 1}]

    with pytest.raises(SchemaResolutionError):
        roundtrip(writer_schema, records, reader_schema=reader_schema)


def test_union_of_float_and_double_keeps_precision():
    """https://github.com/fastavro/fastavro/issues/437"""
    schema = ["float", "string", "double"]
    records = [
        1.0,
        1e200,  # Turns into float("+inf") if parsed as 32 bit float
    ]
    parsed_schema = fastavro.parse_schema(schema)
    assert records == roundtrip(parsed_schema, records)


def test_union_of_float_and_no_double():
    """https://github.com/fastavro/fastavro/issues/437"""
    schema = ["float", "string"]
    records = [1.0]
    parsed_schema = fastavro.parse_schema(schema)
    assert records == roundtrip(parsed_schema, records)


def test_error_if_trying_to_write_the_wrong_number_of_bytes():
    """https://github.com/fastavro/fastavro/issues/522"""
    schema = {"type": "fixed", "size": 2, "name": "fixed"}
    parsed_schema = fastavro.parse_schema(schema)

    records = [b"22", b"1", b"22"]
    with pytest.raises(ValueError):
        roundtrip(parsed_schema, records)

    records = [b"22", b"333", b"22"]
    with pytest.raises(ValueError):
        roundtrip(parsed_schema, records)


def test_tuple_writer_picks_correct_union_path_enum():
    """https://github.com/fastavro/fastavro/issues/536"""
    schema = {
        "type": "record",
        "name": "test_tuple_writer_picks_correct_union_path_enum",
        "fields": [
            {
                "name": "Field",
                "type": [
                    {"type": "string"},
                    {"type": "enum", "name": "Enum", "symbols": ["FOO", "BAR"]},
                ],
            },
        ],
    }

    expected_roundtrip_value = [{"Field": "FOO"}]

    records = [{"Field": ("Enum", "FOO")}]
    parsed_schema = fastavro.parse_schema(schema)
    assert expected_roundtrip_value == roundtrip(
        parsed_schema, records, return_record_name=True
    )

    records = [{"Field": ("string", "FOO")}]
    expected_roundtrip_value = [{"Field": "FOO"}]
    parsed_schema = fastavro.parse_schema(schema)
    assert expected_roundtrip_value == roundtrip(
        parsed_schema, records, return_record_name=True
    )


def test_tuple_writer_picks_correct_union_path_fixed():
    """https://github.com/fastavro/fastavro/issues/536"""
    schema = {
        "type": "record",
        "name": "test_tuple_writer_picks_correct_union_path_fixed",
        "fields": [
            {
                "name": "Field",
                "type": [
                    {"type": "bytes"},
                    {"type": "fixed", "name": "Fixed", "size": 4},
                ],
            },
        ],
    }

    expected_roundtrip_value = [{"Field": b"1234"}]

    records = [{"Field": ("Fixed", b"1234")}]
    parsed_schema = fastavro.parse_schema(schema)
    assert expected_roundtrip_value == roundtrip(
        parsed_schema, records, return_record_name=True
    )

    records = [{"Field": ("bytes", b"1234")}]
    expected_roundtrip_value = [{"Field": b"1234"}]
    parsed_schema = fastavro.parse_schema(schema)
    assert expected_roundtrip_value == roundtrip(
        parsed_schema, records, return_record_name=True
    )


@pytest.mark.parametrize(
    "input_records,expected_roundtrip",
    [
        (
            [
                {
                    "my_1st_union": {"some_field": 1, "-type": "foo"},
                    "my_2nd_union": {"some_field": 2, "-type": "foo"},
                }
            ],
            [
                {
                    "my_1st_union": ("foo", {"some_field": 1}),
                    "my_2nd_union": ("foo", {"some_field": 2}),
                }
            ],
        ),
        (
            [
                {
                    "my_1st_union": {"some_field": 3, "-type": "foo"},
                    "my_2nd_union": {"some_field": 4, "-type": "bar"},
                }
            ],
            [
                {
                    "my_1st_union": ("foo", {"some_field": 3}),
                    "my_2nd_union": ("bar", {"some_field": 4}),
                }
            ],
        ),
        (
            [
                {
                    "my_1st_union": {"some_field": 5, "-type": "bar"},
                    "my_2nd_union": {"some_field": 6, "-type": "foo"},
                }
            ],
            [
                {
                    "my_1st_union": ("bar", {"some_field": 5}),
                    "my_2nd_union": ("foo", {"some_field": 6}),
                }
            ],
        ),
        (
            [
                {
                    "my_1st_union": {"some_field": 7, "-type": "bar"},
                    "my_2nd_union": {"some_field": 8, "-type": "bar"},
                }
            ],
            [
                {
                    "my_1st_union": ("bar", {"some_field": 7}),
                    "my_2nd_union": ("bar", {"some_field": 8}),
                }
            ],
        ),
    ],
)
def test_union_path_picked_with_record_type_hint(input_records, expected_roundtrip):
    """https://github.com/fastavro/fastavro/issues/540"""
    schema = {
        "type": "record",
        "name": "my_record",
        "fields": [
            {
                "name": "my_1st_union",
                "type": [
                    {
                        "name": "foo",
                        "type": "record",
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                    {
                        "name": "bar",
                        "type": "record",
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                ],
            },
            {"name": "my_2nd_union", "type": ["foo", "bar"]},
        ],
    }

    rt_records = roundtrip(
        fastavro.parse_schema(schema), input_records, return_record_name=True
    )
    assert expected_roundtrip == rt_records


def test_non_string_types_raise_type_error():
    """https://github.com/fastavro/fastavro/issues/556"""

    schema = {
        "type": "record",
        "fields": [{"name": "field", "type": "string"}],
        "name": "test_non_string_types_raise_type_error",
    }

    record = {"field": None}

    new_file = BytesIO()
    with pytest.raises(TypeError, match="must be string"):
        fastavro.schemaless_writer(new_file, schema, record)


def test_name_changes_to_full_record():
    """https://github.com/fastavro/fastavro/issues/601"""
    writer_schema = {
        "type": "record",
        "name": "test",
        "fields": [
            {
                "name": "test1",
                "type": {
                    "type": "record",
                    "name": "my_record",
                    "fields": [{"name": "field1", "type": "string"}],
                },
            },
            {"name": "test2", "type": "my_record"},
        ],
    }

    reader_schema = {
        "type": "record",
        "name": "test",
        "fields": [
            {
                "name": "test2",
                "type": {
                    "type": "record",
                    "name": "my_record",
                    "fields": [{"name": "field1", "type": "string"}],
                },
            },
        ],
    }

    records = [{"test1": {"field1": "foo"}, "test2": {"field1": "bar"}}]
    output = roundtrip(writer_schema, records, reader_schema=reader_schema)
    assert output == [{"test2": {"field1": "bar"}}]


def test_record_names_must_match():
    """https://github.com/fastavro/fastavro/issues/601"""
    writer_schema = {
        "type": "record",
        "name": "test",
        "fields": [
            {
                "name": "test1",
                "type": {
                    "type": "record",
                    "name": "my_record",
                    "fields": [{"name": "field1", "type": "string"}],
                },
            },
            {"name": "test2", "type": "my_record"},
        ],
    }

    reader_schema = {
        "type": "record",
        "name": "test",
        "fields": [
            {
                "name": "test2",
                "type": {
                    "type": "record",
                    "name": "different_name",
                    "fields": [{"name": "field1", "type": "string"}],
                },
            },
        ],
    }

    records = [{"test1": {"field1": "foo"}, "test2": {"field1": "bar"}}]
    with pytest.raises(SchemaResolutionError):
        roundtrip(writer_schema, records, reader_schema=reader_schema)


def test_strict_option():
    """https://github.com/fastavro/fastavro/issues/549"""
    schema = {
        "namespace": "namespace",
        "name": "name",
        "type": "record",
        "fields": [
            {"name": "field_1", "type": "boolean"},
            {"name": "field_2", "type": ["null", "string"], "default": None},
        ],
    }

    test_record1 = {"field_1": True, "field_2": "foo", "field_3": "something"}
    test_record2 = {"field_1": True}
    test_record3 = {"field_2": "foo"}

    with pytest.raises(ValueError, match="field_3"):
        roundtrip(schema, [test_record1], writer_kwargs={"strict": True})

    with pytest.raises(ValueError, match="field_2 is specified .*? but missing"):
        roundtrip(schema, [test_record2], writer_kwargs={"strict": True})

    with pytest.raises(ValueError, match="field_1 is specified .*? but missing"):
        roundtrip(schema, [test_record3], writer_kwargs={"strict": True})


def test_strict_allow_default_option():
    """https://github.com/fastavro/fastavro/issues/549"""
    schema = {
        "namespace": "namespace",
        "name": "name",
        "type": "record",
        "fields": [
            {"name": "field_1", "type": "boolean"},
            {"name": "field_2", "type": ["null", "string"], "default": None},
        ],
    }

    test_record1 = {"field_1": True, "field_2": "foo", "field_3": "something"}
    test_record2 = {"field_1": True}
    test_record3 = {"field_2": "foo"}

    with pytest.raises(ValueError, match="field_3"):
        roundtrip(schema, [test_record1], writer_kwargs={"strict_allow_default": True})

    roundtrip(schema, [test_record2], writer_kwargs={"strict_allow_default": True})

    with pytest.raises(ValueError, match="field_1 is specified .*? but missing"):
        roundtrip(schema, [test_record3], writer_kwargs={"strict_allow_default": True})


def test_disable_tuple_notation_option():
    """https://github.com/fastavro/fastavro/issues/548"""
    schema = {
        "namespace": "namespace",
        "name": "name",
        "type": "record",
        "fields": [
            {"name": "foo", "type": ["string", {"type": "array", "items": "string"}]}
        ],
    }

    new_records = roundtrip(
        schema,
        [{"foo": ("string", "0")}],
        writer_kwargs={"disable_tuple_notation": True},
    )
    assert new_records == [{"foo": ["string", "0"]}]


def test_strict_allow_default_bug():
    """https://github.com/fastavro/fastavro/issues/638"""
    schema = {
        "namespace": "namespace",
        "name": "name",
        "type": "record",
        "fields": [{"name": "some_field", "type": "string", "default": "test"}],
    }

    test_record = {"eggs": "eggs"}

    with pytest.raises(ValueError, match="record contains more fields .*? eggs"):
        roundtrip(schema, [test_record], writer_kwargs={"strict_allow_default": True})


def test_nan_default_value():
    """https://github.com/fastavro/fastavro/issues/674"""
    schema = {
        "namespace": "namespace",
        "name": "name",
        "type": "record",
        "fields": [{"name": "some_field", "type": "float", "default": "nan"}],
    }

    test_record = {}

    result_value = roundtrip(schema, [test_record])[0]["some_field"]
    assert math.isnan(result_value)


def test_allow_union_with_bad_default_if_correct_reader_schema_present():
    """https://github.com/fastavro/fastavro/issues/676"""
    writer_schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{"name": "union", "type": ["null", "int"], "default": 0}],
    }

    reader_schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{"name": "union", "type": ["int", "null"], "default": 0}],
    }

    records = [{}]

    # Parse the schema and ignore union default errors like it would have been
    # parsed in the past
    parsed_writer_schema = fastavro.parse_schema(
        writer_schema, _ignore_default_error=True
    )

    roundtrip_records = roundtrip(
        parsed_writer_schema, records, reader_schema=reader_schema
    )
    assert roundtrip_records == [{"union": 0}]


def test_allow_bad_default_if_correct_reader_schema_present():
    """https://github.com/fastavro/fastavro/issues/676"""
    writer_schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{"name": "field1", "type": "int", "default": None}],
    }

    reader_schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{"name": "field1", "type": "int", "default": 0}],
    }

    records = [{"field1": 5}]

    # Parse the schema and ignore union default errors like it would have been
    # parsed in the past
    parsed_writer_schema = fastavro.parse_schema(
        writer_schema, _ignore_default_error=True
    )

    roundtrip_records = roundtrip(
        parsed_writer_schema, records, reader_schema=reader_schema
    )
    assert roundtrip_records == [{"field1": 5}]
