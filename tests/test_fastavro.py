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
