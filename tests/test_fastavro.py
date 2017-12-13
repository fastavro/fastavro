import fastavro
from fastavro.reader import _reader
from fastavro.writer import _writer, Writer

from fastavro.six import MemoryIO

import pytest

import sys
from os.path import join, abspath, dirname, basename
from glob import iglob


data_dir = join(abspath(dirname(__file__)), 'avro-files')

try:
    import snappy  # NOQA
    has_snappy = True
except ImportError:
    has_snappy = False

NO_DATA = set([
    'class org.apache.avro.tool.TestDataFileTools.zerojsonvalues.avro',
    'testDataFileMeta.avro',
])


class NoSeekMemoryIO(object):
    """Shim around MemoryIO which blocks access to everything but read.
    Used to ensure seek API isn't being depended on."""

    def __init__(self, *args):
        self.underlying = MemoryIO(*args)

    def read(self, n):
        return self.underlying.read(n)

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
    schema_migration_reader = fastavro.reader(new_file, reader.schema)
    assert schema_migration_reader.reader_schema == reader.schema
    new_records = list(schema_migration_reader)

    assert new_records == records


def test_not_avro():
    with pytest.raises(ValueError):
        with open(__file__, 'rb') as fo:
            fastavro.reader(fo)


def test_acquaint_schema_rejects_undleclared_name():
    try:
        fastavro.schema.acquaint_schema({
            "type": "record",
            "fields": [{
                "name": "left",
                "type": "Thinger",
            }]
        })
        assert False, 'Never raised'
    except fastavro.schema.UnknownType as e:
        assert 'Thinger' == e.name


def test_acquaint_schema_rejects_unordered_references():
    try:
        fastavro.schema.acquaint_schema({
            "type": "record",
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


def test_acquaint_schema_accepts_nested_namespaces():
    fastavro.schema.acquaint_schema({
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
    assert 'com.example.Inner' in fastavro._writer.SCHEMA_DEFS


def test_acquaint_schema_resolves_references_from_unions():
    fastavro.schema.acquaint_schema({
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
    b_schema = fastavro._writer.SCHEMA_DEFS['com.other.Outer']['fields'][1]
    assert b_schema['type'][1] == "com.other.Inner"


def test_acquaint_schema_accepts_nested_records_from_arrays():
    fastavro.schema.acquaint_schema({
        "fields": [
            {
                "type": {
                    "items": {
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
        "type": "record"
    })
    assert 'Nested' in fastavro._writer.SCHEMA_DEFS


def test_compose_schemas():
    schema_path = join(data_dir, 'Parent.avsc')
    schema = fastavro.schema.load_schema(schema_path)
    assert isinstance(schema, dict)
    assert 'Child' in fastavro._reader.READERS


def test_missing_schema():
    schema_path = join(data_dir, 'ParentMissingChild.avsc')
    with pytest.raises(fastavro.schema.UnknownType):
        fastavro.schema.load_schema(schema_path)


def test_schemaless_writer_and_reader():
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{
            "name": "field",
            "type": {"type": "string"}
        }]
    }
    record = {"field": "test"}
    new_file = MemoryIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, schema)
    assert record == new_record


def test_default_values():
    schema = {
        "type": "record",
        "fields": [{
            "name": "default_field",
            "type": "string",
            "default": "default_value"
        }]
    }
    new_file = MemoryIO()
    records = [{}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    new_records = list(new_reader)
    assert new_records == [{"default_field": "default_value"}]


def test_nullable_values():
    schema = {
        "type": "record",
        "fields": [{
            "name": "nullable_field",
            "type": ["string", "null"]
        }, {
            "name": "field",
            "type": "string"
        }
        ]
    }
    new_file = MemoryIO()
    records = [{"field": "val"}, {"field": "val", "nullable_field": "no_null"}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    new_records = list(new_reader)
    print(new_records)
    assert new_records == [{'nullable_field': None, 'field': 'val'}, {
        'nullable_field': 'no_null', 'field': 'val'}]


def test_boolean_roundtrip():
    schema = {
        "type": "record",
        "fields": [{
            "name": "field",
            "type": "boolean"
        }]
    }
    record = {"field": True}
    new_file = MemoryIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, schema)
    assert record == new_record

    record = {"field": False}
    new_file = MemoryIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, schema)
    assert record == new_record


def test_metadata():
    schema = {
        "type": "record",
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

    new_file = MemoryIO()
    records = [{"a": ("B", {"b": "test"})}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    new_records = list(new_reader)
    assert new_records == [{"a": {"b": "test"}}]


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

    new_file = MemoryIO()
    records = [{"b": {"c": "test"}}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    new_records = list(new_reader)
    assert new_records == records

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

    new_file = MemoryIO()
    records = [{"a": {"b": {"c": 1}}, "aa": {"b": {"c": 2}}}]
    fastavro.writer(new_file, other_schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_remove_field():
    schema = {
        "type": "record",
        "fields": [{
            "name": "test",
            "type": "string",
        }]
    }

    new_schema = {
        "type": "record",
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
        "fields": []
    }

    new_schema = {
        "type": "record",
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
        "fields": [{
            "name": "test",
            "type": ["string", "int"],
        }]
    }

    new_schema = {
        "type": "record",
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
        "fields": [{
            "name": "test",
            "type": ["string", "int"]
        }]
    }

    new_schema = {
        "type": "record",
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
        "fields": [{
            "name": "test",
            "type": "int"
        }]
    }

    new_schema = {
        "type": "record",
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
        "fields": [{
            "name": "test",
            "type": "boolean"
        }]
    }

    new_schema = {
        "type": "record",
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

    with pytest.raises(fastavro._reader.SchemaResolutionError):
        list(new_reader)


def test_schema_migration_array_failure():
    schema = {
        "type": "record",
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

    with pytest.raises(fastavro._reader.SchemaResolutionError):
        list(new_reader)


def test_schema_migration_maps_failure():
    schema = {
        "type": "record",
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
    with pytest.raises(fastavro._reader.SchemaResolutionError):
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
    with pytest.raises(fastavro._reader.SchemaResolutionError):
        list(new_reader)


def test_schema_migration_schema_mismatch():
    schema = {
        "type": "record",
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
    with pytest.raises(fastavro._reader.SchemaResolutionError):
        list(new_reader)


def test_empty():
    io = MemoryIO()
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'type': 'boolean', 'name': 'a'}
        ],
    }
    with pytest.raises(EOFError):
        fastavro.load(io, schema)


def test_no_default():
    io = MemoryIO()
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'type': 'boolean', 'name': 'a'}
        ],
    }
    with pytest.raises(ValueError):
        fastavro.writer(io, schema, [{}])


@pytest.mark.skip(reason='FIXME: Add tests for write validator argument')
def test_validator():
    pass


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

    new_file = MemoryIO()
    records = [
        {u'time': 809066167221092352},
    ]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    new_records = list(new_reader)
    assert new_records == [{u'time': 809066167221092352}]


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
    new_file = MemoryIO()
    fastavro.writer(new_file, schema, data)
    new_file.seek(0)

    new_reader = fastavro.reader(new_file)
    new_records = list(new_reader)
    assert new_records == data


def test_dump_load(tmpdir):
    """
    Write an Avro record to a file using the dump() function and loads it back
    using the load() function.
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
    record = {"field": "foobar"}

    temp_path = tmpdir.join('test_dump.avro')
    with temp_path.open('wb') as fo:
        fastavro.dump(fo, record, schema)

    with temp_path.open('rb') as fo:
        new_record = fastavro.load(fo, schema)

    assert record == new_record
