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


def test_int_and_long_unions():
    schema = {
        "type": "record",
        "name": "LongTest",
        "namespace": "test",
        "fields": [{
            "name": "field",
            "type": [{"type": "int"}, {"type": "long"}]
        }]
    }
    records = [{"field": long(123)}]
    fo = MemoryIO()
    fastavro.writer(fo, schema, records)

    fo.seek(0)
    reader = fastavro.reader(fo)
    new_records = list(reader)

    assert type(new_records[0]['field']) == type(records[0]['field'])
