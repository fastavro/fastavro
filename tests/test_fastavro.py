import fastavro

from fastavro.six import MemoryIO
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


def check(filename):
    with open(filename, 'rb') as fo:
        reader = fastavro.reader(fo)
        assert hasattr(reader, 'schema'), 'no schema on file'

        if basename(filename) in NO_DATA:
            return

        records = list(reader)
        assert len(records) > 0, 'no records found'

    new_file = MemoryIO()
    fastavro.writer(new_file, reader.schema, records, reader.codec)

    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    assert hasattr(new_reader, 'schema'), "schema wasn't written"
    assert new_reader.schema == reader.schema
    assert new_reader.codec == reader.codec
    new_records = list(new_reader)

    assert new_records == records

    # Test schema migration with the same schema
    new_file.seek(0)
    schema_migration_reader = fastavro.reader(new_file, reader.schema)
    assert schema_migration_reader.reader_schema == reader.schema
    new_records = list(schema_migration_reader)

    assert new_records == records


def test_fastavro():
    for filename in iglob(join(data_dir, '*.avro')):
        if (not has_snappy) and ('snappy' in filename):
            continue
        yield check, filename


def test_not_avro():
    try:
        with open(__file__, 'rb') as fo:
            fastavro.reader(fo)
        assert False, 'opened non avro file'
    except ValueError:
        pass


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
    try:
        list(new_reader)
    except fastavro._reader.SchemaResolutionError:
        pass
    else:
        assert False


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
    try:
        list(new_reader)
    except fastavro._reader.SchemaResolutionError:
        pass
    else:
        assert False


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
    try:
        list(new_reader)
    except fastavro._reader.SchemaResolutionError:
        pass
    else:
        assert False


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
    try:
        list(new_reader)
    except fastavro._reader.SchemaResolutionError:
        pass
    else:
        assert False


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
    try:
        list(new_reader)
    except fastavro._reader.SchemaResolutionError:
        pass
    else:
        assert False
