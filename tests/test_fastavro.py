import fastavro
from fastavro.read import _read as _reader
from fastavro.write import _write as _writer, Writer
from fastavro._schema_common import SCHEMA_DEFS

from fastavro.six import MemoryIO

import pytest

import sys
import traceback
import zipfile
from collections import OrderedDict
from os.path import join, abspath, dirname, basename
from glob import iglob

pytestmark = pytest.mark.usefixtures("clean_schemas")

data_dir = join(abspath(dirname(__file__)), 'avro-files')

try:
    import snappy  # NOQA

    has_snappy = True
except ImportError:
    has_snappy = False

NO_DATA = {
    'class org.apache.avro.tool.TestDataFileTools.zerojsonvalues.avro',
    'testDataFileMeta.avro',
}


def roundtrip(schema, records, pass_schema_to_reader=False):
    new_file = MemoryIO()
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)

    if pass_schema_to_reader:
        reader = fastavro.reader(new_file, schema)
    else:
        reader = fastavro.reader(new_file)

    new_records = list(reader)
    return new_records


class NoSeekMemoryIO(object):
    """Shim around MemoryIO which blocks access to everything but read.
    Used to ensure seek API isn't being depended on."""

    def __init__(self, *args):
        self.underlying = MemoryIO(*args)

    def read(self, n):
        return self.underlying.read(n)

    def tell(self):
        return self.underlying.tell()

    def seek(self, *args):
        raise AssertionError("fastavro reader should not depend on seek")


def _test_files():
    for filename in iglob(join(data_dir, '*.avro')):
        if (not has_snappy) and ('snappy' in filename):
            continue
        yield filename


@pytest.mark.parametrize('filename', _test_files())
def test_file(filename):
    with open(filename, 'rb') as fo:
        reader = fastavro.reader(fo)
        assert hasattr(reader, 'schema'), 'no schema on file'

        if basename(filename) in NO_DATA:
            return

        records = list(reader)
        assert len(records) > 0, 'no records found'

    new_file = MemoryIO()
    fastavro.writer(new_file, reader.schema, records, reader.codec)
    new_file_bytes = new_file.getvalue()

    new_file = NoSeekMemoryIO(new_file_bytes)
    new_reader = fastavro.reader(new_file)
    assert hasattr(new_reader, 'schema'), "schema wasn't written"
    assert new_reader.schema == reader.schema
    assert new_reader.codec == reader.codec
    new_records = list(new_reader)

    assert new_records == records

    # Test schema migration with the same schema
    new_file = NoSeekMemoryIO(new_file_bytes)
    schema_migration_reader = fastavro.reader(new_file, reader.writer_schema)
    assert schema_migration_reader.reader_schema == reader.writer_schema
    new_records = list(schema_migration_reader)

    assert new_records == records


def test_not_avro():
    with pytest.raises(ValueError):
        with open(__file__, 'rb') as fo:
            fastavro.reader(fo)


def test_parse_schema_rejects_undleclared_name():
    try:
        fastavro.parse_schema({
            "type": "record",
            "name": "test_parse_schema_rejects_undleclared_name",
            "fields": [{
                "name": "left",
                "type": "Thinger",
            }]
        })
        assert False, 'Never raised'
    except fastavro.schema.UnknownType as e:
        assert 'Thinger' == e.name


def test_parse_schema_rejects_unordered_references():
    try:
        fastavro.parse_schema({
            "type": "record",
            "name": "test_parse_schema_rejects_unordered_references",
            "fields": [{
                "name": "left",
                "type": "Thinger"
            }, {
                "name": "right",
                "type": {
                    "type": "record",
                    "name": "Thinger",
                    "fields": [{
                        "name": "the_thing",
                        "type": "string"
                    }]
                }
            }]
        })
        assert False, 'Never raised'
    except fastavro.schema.UnknownType as e:
        assert 'Thinger' == e.name


def test_parse_schema_accepts_nested_namespaces():
    parsed_schema = fastavro.parse_schema({
        "namespace": "com.example",
        "name": "Outer",
        "type": "record",
        "fields": [{
            "name": "a",
            "type": {
                "type": "record",
                "name": "Inner",
                "fields": [{
                    "name": "the_thing",
                    "type": "string"
                }]
            }
        }, {
            "name": "b",
            # This should resolve to com.example.Inner because of the
            # `namespace` of the enclosing record.
            "type": "Inner"
        }, {
            "name": "b",
            "type": "com.example.Inner"
        }]
    })
    assert "com.example.Inner" == parsed_schema["fields"][0]["type"]["name"]
    assert "com.example.Inner" == parsed_schema["fields"][1]["type"]


def test_parse_schema_resolves_references_from_unions():
    parsed_schema = fastavro.parse_schema({
        "namespace": "com.other",
        "name": "Outer",
        "type": "record",
        "fields": [{
            "name": "a",
            "type": ["null", {
                "type": "record",
                "name": "Inner",
                "fields": [{
                    "name": "the_thing",
                    "type": "string"
                }]
            }]
        }, {
            "name": "b",
            # This should resolve to com.example.Inner because of the
            # `namespace` of the enclosing record.
            "type": ["null", "Inner"]
        }]
    })
    assert "com.other.Inner" == parsed_schema["fields"][1]["type"][1]


def test_parse_schema_accepts_nested_records_from_arrays():
    parsed_schema = fastavro.parse_schema({
        "fields": [
            {
                "type": {
                    "items": {
                        "type": "record",
                        "fields": [{"type": "string", "name": "text"}],
                        "name": "Nested"
                    },
                    "type": "array",
                },
                "name": "multiple"
            },
            {
                "type": {
                    "type": "array",
                    "items": "Nested"
                },
                "name": "single"
            }
        ],
        "type": "record",
        "name": "test_parse_schema_accepts_nested_records_from_arrays",
    })
    assert "Nested" == parsed_schema["fields"][1]["type"]["items"]


def test_compose_schemas():
    schema_path = join(data_dir, 'Parent.avsc')
    fastavro.schema.load_schema(schema_path)


def test_reading_after_writing_with_load_schema():
    schema_path = join(data_dir, 'Parent.avsc')
    schema = fastavro.schema.load_schema(schema_path)

    records = [{'child': {}, 'child1': {}}]

    new_file = MemoryIO()
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)

    # Clean the Child and Parent entries so we are forced to get them from the
    # schema
    del SCHEMA_DEFS['Child']
    del SCHEMA_DEFS['Child1']
    del SCHEMA_DEFS['Parent']

    reader = fastavro.reader(new_file)
    new_records = list(reader)
    assert new_records == records


def test_missing_schema():
    schema_path = join(data_dir, 'ParentMissingChild.avsc')
    with pytest.raises(fastavro.schema.UnknownType):
        fastavro.schema.load_schema(schema_path)


def test_default_values():
    schema = {
        "type": "record",
        "name": "test_default_values",
        "fields": [{
            "name": "default_field",
            "type": "string",
            "default": "default_value"
        }]
    }
    records = [{}]

    new_records = roundtrip(schema, records)
    assert new_records == [{"default_field": "default_value"}]


def test_nullable_values():
    schema = {
        "type": "record",
        "name": "test_nullable_values",
        "fields": [{
            "name": "nullable_field",
            "type": ["string", "null"]
        }, {
            "name": "field",
            "type": "string"
        }
        ]
    }
    records = [{"field": "val"}, {"field": "val", "nullable_field": "no_null"}]

    new_records = roundtrip(schema, records)
    assert new_records == [{'nullable_field': None, 'field': 'val'}, {
        'nullable_field': 'no_null', 'field': 'val'}]


def test_metadata():
    schema = {
        "type": "record",
        "name": "test_metadata",
        "fields": []
    }

    new_file = MemoryIO()
    records = [{}]
    metadata = {'key': 'value'}
    fastavro.writer(new_file, schema, records, metadata=metadata)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    assert new_reader.metadata['key'] == metadata['key']


def test_write_union_shortcut():
    schema = {
        "type": "record",
        "name": "A",
        "fields": [{
            "name": "a",
            "type": [
                {
                    "type": "record",
                    "name": "B",
                    "fields": [{
                        "name": "b",
                        "type": "string"
                    }]
                },
                {
                    "type": "record",
                    "name": "C",
                    "fields": [{
                        "name": "c",
                        "type": "string"
                    }]
                }
            ]
        }]
    }

    records = [{"a": ("B", {"b": "test"})}]

    assert [{"a": {"b": "test"}}] == roundtrip(schema, records)


def test_repo_caching_issue():
    schema = {
        "type": "record",
        "name": "B",
        "fields": [{
            "name": "b",
            "type": {
                "type": "record",
                "name": "C",
                "fields": [{
                    "name": "c",
                    "type": "string"
                }]
            }
        }]
    }

    records = [{"b": {"c": "test"}}]

    assert records == roundtrip(schema, records)

    other_schema = {
        "name": "A",
        "type": "record",
        "fields": [{
            "name": "a",
            "type": {
                "type": "record",
                "name": "B",
                "fields": [{
                    "name": "b",
                    "type": {
                        "type": "record",
                        "name": "C",
                        "fields": [{
                            "name": "c",
                            "type": "int"
                        }]
                    }
                }]
            }
        }, {
            "name": "aa",
            "type": "B"
        }]
    }

    records = [{"a": {"b": {"c": 1}}, "aa": {"b": {"c": 2}}}]

    assert records == roundtrip(other_schema, records)


def test_schema_migration_remove_field():
    schema = {
        "type": "record",
        "name": "test_schema_migration_remove_field",
        "fields": [{
            "name": "test",
            "type": "string",
        }]
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_remove_field_new",
        "fields": []
    }

    new_file = MemoryIO()
    records = [{'test': 'test'}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == [{}]


def test_schema_migration_add_default_field():
    schema = {
        "type": "record",
        "name": "test_schema_migration_add_default_field",
        "fields": []
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_add_default_field_new",
        "fields": [{
            "name": "test",
            "type": "string",
            "default": "default",
        }]
    }

    new_file = MemoryIO()
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
        "fields": [{
            "name": "test",
            "type": ["string", "int"],
        }]
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_type_promotion_new",
        "fields": [{
            "name": "test",
            "type": ["float", "string"],
        }]
    }

    new_file = MemoryIO()
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
        "fields": [{
            "name": "test",
            "type": {
                "type": "map",
                "values": ["string", "int"]
            },
        }]
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_maps_with_union_promotion_new",
        "fields": [{
            "name": "test",
            "type": {
                "type": "map",
                "values": ["string", "long"]
            },
        }]
    }

    new_file = MemoryIO()
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
        "fields": [{
            "name": "test",
            "type": {
                "type": "array",
                "items": ["boolean", "long"]
            },
        }]
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_array_with_union_promotion_new",
        "fields": [{
            "name": "test",
            "type": {
                "type": "array",
                "items": ["string", "float"]
            },
        }]
    }

    new_file = MemoryIO()
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
        "fields": [{
            "name": "test",
            "type": ["string", "int"]
        }]
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_writer_union_new",
        "fields": [{
            "name": "test",
            "type": "int"
        }]
    }

    new_file = MemoryIO()
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
        "fields": [{
            "name": "test",
            "type": "int"
        }]
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_reader_union_new",
        "fields": [{
            "name": "test",
            "type": ["string", "int"]
        }]
    }

    new_file = MemoryIO()
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
        "fields": [{
            "name": "test",
            "type": "boolean"
        }]
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_union_failure_new",
        "fields": [{
            "name": "test",
            "type": ["string", "int"]
        }]
    }

    new_file = MemoryIO()
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
        "fields": [{
            "name": "test",
            "type": {
                "type": "array",
                "items": ["string", "int"]
            },
        }]
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_array_failure_new",
        "fields": [{
            "name": "test",
            "type": {
                "type": "array",
                "items": ["string", "boolean"]
            },
        }]
    }

    new_file = MemoryIO()
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
        "fields": [{
            "name": "test",
            "type": {
                "type": "map",
                "values": "string"
            },
        }]
    }

    new_schema = {
        "type": "record",
        "name": "test_schema_migration_maps_failure_new",
        "fields": [{
            "name": "test",
            "type": {
                "type": "map",
                "values": "long"
            },
        }]
    }

    new_file = MemoryIO()
    records = [{"test": {"foo": "a"}}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    with pytest.raises(fastavro.read.SchemaResolutionError):
        list(new_reader)


def test_schema_migration_enum_failure():
    schema = {
        "type": "enum",
        "name": "test",
        "symbols": ["FOO", "BAR"],
    }

    new_schema = {
        "type": "enum",
        "name": "test",
        "symbols": ["BAZ", "BAR"],
    }

    new_file = MemoryIO()
    records = ["FOO"]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    with pytest.raises(fastavro.read.SchemaResolutionError):
        list(new_reader)


def test_schema_migration_schema_mismatch():
    schema = {
        "type": "record",
        "name": "test_schema_migration_schema_mismatch",
        "fields": [{
            "name": "test",
            "type": "string",
        }]
    }

    new_schema = {
        "type": "enum",
        "name": "test",
        "symbols": ["FOO", "BAR"],
    }

    new_file = MemoryIO()
    records = [{"test": "test"}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    with pytest.raises(fastavro.read.SchemaResolutionError):
        list(new_reader)


def test_empty():
    io = MemoryIO()
    with pytest.raises(ValueError) as exc:
        fastavro.reader(io)

    assert 'cannot read header - is it an avro file?' in str(exc)


def test_no_default():
    io = MemoryIO()
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'type': 'boolean', 'name': 'a'}
        ],
    }
    with pytest.raises(ValueError) as exc:
        fastavro.writer(io, schema, [{}])

    assert 'no value and no default' in str(exc)


def test_is_avro_str():
    for path in iglob('%s/*.avro' % data_dir):
        assert fastavro.is_avro(path)
    assert not fastavro.is_avro(__file__)


def test_is_avro_fo():
    for path in iglob('%s/*.avro' % data_dir):
        with open(path, 'rb') as fp:
            assert fastavro.is_avro(fp)
    with open(__file__, 'rb') as fp:
        assert not fastavro.is_avro(fp)


def test_write_long_union_type():
    schema = {
        'name': 'test_name',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {'name': 'time', 'type': ['null', 'long']},
        ],
    }

    records = [
        {u'time': 809066167221092352},
    ]

    assert records == roundtrip(schema, records)


def test_cython_python():
    # Since Cython and Python implement the same behavior, it is possible for
    # build errors or coding errors to accidentally result in using the wrong
    # one. This is bad, because the pure Python version is faster in Pypy,
    # while the Cython version is faster in CPython. This test verifies the
    # correct reader and writer implementations are used.
    if hasattr(sys, 'pypy_version_info'):
        # Pypy should not use Cython.
        assert not hasattr(_reader, 'CYTHON_MODULE')
        assert not hasattr(_writer, 'CYTHON_MODULE')
    else:
        # CPython should use Cython.
        assert getattr(_reader, 'CYTHON_MODULE')
        assert getattr(_writer, 'CYTHON_MODULE')


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
            {
                "name": "field1",
                "type": {"type": "string"}
            },
            {
                "name": "field2",
                "type": {"type": "int"}
            }
        ]
    }
    records = [
        {"field1": "test1", "field2": -1},
        {"field1": "test2", "field2": 5}
    ]

    temp_path = tmpdir.join('test_writer_class.avro')
    with temp_path.open('wb') as fo:
        w = Writer(fo, schema, codec='deflate')

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
    new_reader = fastavro.reader(temp_path.open('rb'))
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
            {
                "name": "field1",
                "type": {"type": "string"}
            },
            {
                "name": "field2",
                "type": {"type": "int"}
            }
        ]
    }
    records = [
        {"field1": "test1", "field2": -1},
        {"field1": "test2", "field2": 5}
    ]

    temp_path = tmpdir.join('test_writer_class.avro')
    with temp_path.open('wb') as fo:
        w = Writer(fo, schema, codec='deflate', sync_interval=0)

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
    new_reader = fastavro.reader(temp_path.open('rb'))
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
        "fields": [
            {
                "name": "field",
                "type": {"type": "string"}
            }
        ]
    }
    records = []

    def _append_record(writer_):
        record = {"field": "test{}".format(len(records))}
        records.append(record)
        writer_.write(record)

    temp_paths = [
        tmpdir.join('test_writer_class1.avro'),
        tmpdir.join('test_writer_class2.avro')]
    interim_record_counts = []

    # First file: Write records until block_count goes back to 0 for the second
    # time.
    with temp_paths[0].open('wb') as fo:
        w = Writer(fo, schema, codec='deflate')
        _append_record(w)
        while w.block_count > 0:
            _append_record(w)
        _append_record(w)
        while w.block_count > 0:
            _append_record(w)
        w.flush()
    interim_record_counts.append(len(records))

    # Second file: 100 records
    with temp_paths[1].open('wb') as fo:
        w = Writer(fo, schema, codec='deflate')
        for i in range(100):
            _append_record(w)
        w.flush()
    interim_record_counts.append(len(records))

    assert interim_record_counts[1] == interim_record_counts[0] + 100

    # Read the records to verify they were written correctly.
    new_records = []
    new_interim_record_counts = []
    for temp_path in temp_paths:
        new_reader = fastavro.reader(temp_path.open('rb'))
        new_records += list(new_reader)
        new_interim_record_counts.append(len(new_records))
    assert new_records == records
    assert interim_record_counts == new_interim_record_counts


def test_union_records():
    #
    schema = {
        'name': 'test_name',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {
                'name': 'val',
                'type': [
                    {
                        'name': 'a',
                        'namespace': 'common',
                        'type': 'record',
                        'fields': [
                            {'name': 'x', 'type': 'int'},
                            {'name': 'y', 'type': 'int'},
                        ],
                    },
                    {
                        'name': 'b',
                        'namespace': 'common',
                        'type': 'record',
                        'fields': [
                            {'name': 'x', 'type': 'int'},
                            {'name': 'y', 'type': 'int'},
                            {'name': 'z', 'type': ['null', 'int']},
                        ],
                    }
                ]
            }
        ]
    }

    data = [{
        'val': {
            'x': 3,
            'y': 4,
            'z': 5,
        }
    }]

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
        "fields": [
            {
                "name": "field",
                "type": {"type": "string"}
            }
        ]
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
        "fields": [{
            "name": "test",
            "type": {
                "type": "map",
                "values": ["string", "int"]
            },
        }]
    }

    map_ = OrderedDict()
    map_["foo"] = 1
    records = [{"test": map_}]

    assert records == roundtrip(schema, records)


@pytest.mark.skipif(
    not hasattr(_writer, 'CYTHON_MODULE'),
    reason='Cython-specific test'
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
        "fields": [
            {
                "name": "field",
                "type": {"type": "int"}
            }
        ]
    }

    # Test with two different bad records. One is a regular dict, and the other
    # is an OrderedDict. Both have a bad value (string where the schema
    # declares an int).
    test_records = [{'field': 'foobar'}]
    record = OrderedDict()
    record["field"] = "foobar"
    test_records.append(record)

    expected_write_record_stack_traces = [
        # For the regular dict, fails by reraising an error accessing
        # 'd_datum', a variable that only gets a value if the record is an
        # actual dict.
        [
            'write_data(fo, d_datum.get('
        ],
        # For the OrderedDict, fails directly when accessing 'datum', the
        # variable that is used if the record is *not* an actual dict.
        [
            'write_data(fo, datum.get('
        ]
    ]

    for test_record, expected_write_record_stack_trace in zip(
            test_records,
            expected_write_record_stack_traces):
        new_file = MemoryIO()
        records = [test_record]
        try:
            fastavro.writer(new_file, schema, records)
            assert False, "Should've raised TypeError"
        except TypeError:
            _, _, tb = sys.exc_info()
            stack = traceback.extract_tb(tb)
            filtered_stack = [
                frame[3] for frame in stack if 'write_record' in frame[2]]
            assert filtered_stack == expected_write_record_stack_trace


@pytest.mark.skipif(
    not hasattr(_writer, 'CYTHON_MODULE'),
    reason='Cython-specific test'
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
        "fields": [{
            "name": "test",
            "type": {
                "type": "map",
                "values": "int"
            },
        }]
    }

    # Test with two different bad records. One is a regular dict, and the other
    # is an OrderedDict. Both have a bad value (string where the schema
    # declares an int).
    test_records = [{'test': {'foo': 'bar'}}]
    map_ = OrderedDict()
    map_["foo"] = "bar"
    test_records.append({'test': map_})

    filtered_stacks = []
    for test_record in test_records:
        new_file = MemoryIO()
        records = [test_record]
        try:
            fastavro.writer(new_file, schema, records)
            assert False, "Should've raised TypeError"
        except TypeError:
            _, _, tb = sys.exc_info()
            stack = traceback.extract_tb(tb)
            filtered_stack = [
                frame[1] for frame in stack if 'write_map' in frame[2]]
            filtered_stacks.append(filtered_stack)

    # Because of the special-case code for dicts, the two stack traces should
    # be different, indicating the exception occurred at a different line
    # number.
    assert filtered_stacks[0] != filtered_stacks[1]


def test_write_union_tuple_primitive():
    '''
    Test that when we can use tuple style of writing unions
    (see function `write_union` in `_write`) with primitives
     not only with records.
    '''

    schema = {
        'name': 'test_name',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {
                'name': 'val',
                'type': ['string', 'int']
            }
        ]
    }

    data = [
        {"val": ("int", 1)},
        {"val": ("string", "string")},
    ]

    expected_data = [
        {"val": 1},
        {"val": "string"},
    ]

    new_file = MemoryIO()
    fastavro.writer(new_file, schema, data)
    new_file.seek(0)

    new_reader = fastavro.reader(new_file)
    new_records = list(new_reader)

    assert new_records == expected_data


def test_doubles_set_to_zero_on_windows():
    """https://github.com/fastavro/fastavro/issues/154"""

    schema = {
        'doc': 'A weather reading.',
        'name': 'Weather',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {'name': 'station', 'type': 'string'},
            {'name': 'time', 'type': 'long'},
            {'name': 'temp', 'type': 'int'},
            {'name': 'test_float', 'type': 'double'}
        ]
    }

    records = [{
        'station': '011990-99999',
        'temp': 0,
        'test_float': 0.21334215134123513,
        'time': -714214260,
    }, {
        'station': '011990-99999',
        'temp': 22,
        'test_float': 0.21334215134123513,
        'time': -714213259,
    }, {
        'station': '011990-99999',
        'temp': -11,
        'test_float': 0.21334215134123513,
        'time': -714210269,
    }, {
        'station': '012650-99999',
        'temp': 111,
        'test_float': 0.21334215134123513,
        'time': -714208170,
    }]

    assert records == roundtrip(schema, records)


def test_string_not_treated_as_array():
    """https://github.com/fastavro/fastavro/issues/166"""

    schema = {
        'type': 'record',
        'fields': [{
            'name': 'description',
            "type": [
                "null",
                {
                    "type": "array",
                    "items": "string"
                },
                "string"
            ],
        }],
        "name": "description",
        "doc": "A description of the thing."
    }

    records = [{
        'description': 'value',
    }, {
        'description': ['an', 'array']
    }]

    assert records == roundtrip(schema, records)


def test_schema_is_custom_dict_type():
    """https://github.com/fastavro/fastavro/issues/168"""

    class CustomDict(dict):
        pass

    schema = {
        'type': 'record',
        'fields': [{
            'name': 'description',
            "type": [
                "null",
                {
                    "type": "array",
                    "items": "string"
                },
                "string"
            ],
        }],
        "name": "description",
        "doc": "A description of the thing."
    }
    other_type_schema = CustomDict(schema)

    record = {
        'description': 'value',
    }

    new_file = MemoryIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, other_type_schema)
    assert record == new_record


def test_long_bounds():
    schema = {
        'name': 'test_long_bounds',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {'name': 'time', 'type': 'long'},
        ],
    }

    records = [
        {'time': (1 << 63) - 1},
        {'time': -(1 << 63)},
    ]

    assert records == roundtrip(schema, records)


def test_py37_runtime_error():
    """On Python 3.7 this test would cause the StopIteration to get raised as
    a RuntimeError.

    See https://www.python.org/dev/peps/pep-0479/
    """
    weather_file = join(data_dir, 'weather.avro')

    zip_io = MemoryIO()
    with zipfile.ZipFile(zip_io, mode='w') as zio:
        zio.write(weather_file, arcname='weather')

    with zipfile.ZipFile(zip_io) as zio:
        with zio.open('weather') as fo:
            # Need to read fo into a bytes buffer for python versions less
            # than 3.7
            reader = fastavro.reader(MemoryIO(fo.read()))
            list(reader)


def test_eof_error():
    schema = {
        "type": "record",
        "name": "test_eof_error",
        "fields": [{
            "name": "test",
            "type": "float",
        }]
    }

    new_file = MemoryIO()
    record = {"test": 1.234}
    fastavro.schemaless_writer(new_file, schema, record)

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
                    }
                ]
            }
        ]
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
        'namespace': 'test.avro.training',
        'name': 'SomeMessage',
        'type': 'record',
        'fields': [{
            'name': 'is_error',
            'type': 'boolean',
            'default': False,
        }, {
            'name': 'outcome',
            'type': [{
                'type': 'record',
                'name': 'SomeMessage',
                'fields': [],
            }, {
                'type': 'record',
                'name': 'ErrorRecord',
                'fields': [{
                    'name': 'errors',
                    'type': {'type': 'map', 'values': 'string'},
                    'doc': 'doc',
                }]
            }]
        }]
    }

    records = [{
        'is_error': True,
        'outcome': {
            'errors': {
                'field_1': 'some_message',
                'field_2': 'some_other_message'
            }
        }
    }]

    assert records == roundtrip(schema, records, pass_schema_to_reader=True)


def test_helpful_error_when_a_single_record_is_passed_to_writer():
    """https://github.com/fastavro/fastavro/issues/254"""
    schema = {
        'namespace': 'namespace',
        'name': 'name',
        'type': 'record',
        'fields': [{
            'name': 'is_error',
            'type': 'boolean',
        }]
    }

    record = {
        'is_error': True,
    }

    new_file = MemoryIO()
    with pytest.raises(ValueError) as exc:
        fastavro.writer(new_file, schema, record)

    assert "argument should be an iterable, not dict" in str(exc)


def test_embedded_records_get_namespaced_correctly():
    schema = {
        'namespace': 'test',
        'name': 'OuterName',
        'type': 'record',
        'fields': [{
            'name': 'data',
            'type': [{
                'type': 'record',
                'name': 'UUID',
                'fields': [{
                    'name': 'uuid',
                    'type': 'string'
                }]
            }, {
                'type': 'record',
                'name': 'Abstract',
                'fields': [{
                    'name': 'uuid',
                    'type': 'UUID',
                }],
            }, {
                'type': 'record',
                'name': 'Concrete',
                'fields': [{
                    'name': 'abstract',
                    'type': 'Abstract'
                }, {
                    'name': 'custom',
                    'type': 'string',
                }],
            }]
        }]
    }

    records = [{
        'data': {
            'abstract': {
                'uuid': {'uuid': 'some_uuid'}
            },
            'custom': 'some_string'
        }
    }]

    assert records == roundtrip(schema, records)


def test_null_defaults_are_not_used():
    """https://github.com/fastavro/fastavro/issues/272"""
    schema = [{
        "type": "record",
        "name": "A",
        "fields": [{"name": "foo", "type": ["string", "null"]}]
    }, {
        "type": "record",
        "name": "B",
        "fields": [{"name": "bar", "type": ["string", "null"]}]
    }, {
        "type": "record",
        "name": "AOrB",
        "fields": [{"name": "entity", "type": ["A", "B"]}]
    }]

    datum_to_read = {'entity': {'foo': 'this is an instance of schema A'}}

    assert [datum_to_read] == roundtrip(schema, [datum_to_read])


def test_union_schema_ignores_extra_fields():
    """https://github.com/fastavro/fastavro/issues/274"""
    schema = [{
        "type": "record",
        "name": "A",
        "fields": [{"name": "name", "type": "string"}]
    }, {
        "type": "record",
        "name": "B",
        "fields": [{"name": "other_name", "type": "string"}]
    }]

    records = [{"name": "abc", "other": "asd"}]

    assert [{"name": "abc"}] == roundtrip(schema, records)


def test_appending_records(tmpdir):
    """https://github.com/fastavro/fastavro/issues/276"""
    schema = {
        "type": "record",
        "name": "test_appending_records",
        "fields": [{
            "name": "field",
            "type": "string",
        }]
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
        "fields": [{
            "name": "field",
            "type": "string",
        }]
    }

    stream = MemoryIO()

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
        "fields": [{
            "name": "field",
            "type": "string",
        }]
    }

    test_file = str(tmpdir.join("test.avro"))

    with open(test_file, "wb") as new_file:
        fastavro.writer(new_file, schema, [{"field": "foo"}])

    with open(test_file, "ab") as new_file:
        with pytest.raises(ValueError) as exc:
            fastavro.writer(new_file, schema, [{"field": "bar"}])

        assert "you must use the 'a+' mode, not just 'a'" in str(exc)


def test_appending_records_different_schema_fails(tmpdir):
    """https://github.com/fastavro/fastavro/issues/276"""
    schema = {
        "type": "record",
        "name": "test_appending_records_different_schema_fails",
        "fields": [{
            "name": "field",
            "type": "string",
        }]
    }

    test_file = str(tmpdir.join("test.avro"))

    with open(test_file, "wb") as new_file:
        fastavro.writer(new_file, schema, [{"field": "foo"}])

    different_schema = {
        "type": "record",
        "name": "test_appending_records",
        "fields": [{
            "name": "field",
            "type": "int",
        }]
    }

    with open(test_file, "a+b") as new_file:
        with pytest.raises(ValueError) as exc:
            fastavro.writer(new_file, different_schema, [{"field": 1}])

        assert "does not match file writer_schema" in str(exc)
