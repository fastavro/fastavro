import fastavro

from os.path import join, abspath, dirname, basename
from glob import iglob

data_dir = join(abspath(dirname(__file__)), 'avro-files')

NO_DATA = set([
    'class org.apache.avro.tool.TestDataFileTools.zerojsonvalues.avro',
    'testDataFileMeta.avro',
])


def check(filename):
    with open(filename, 'rb') as fo:
        reader = fastavro.reader(fo)
        assert hasattr(reader, 'schema'), 'no schema'

        if basename(filename) in NO_DATA:
            return

        num_records = sum(1 for record in reader)
        assert num_records > 0, 'no records found'


def test_fastavro():
    for filename in iglob(join(data_dir, '*.avro')):
        yield check, filename


def test_not_avro():
    try:
        with open(__file__) as fo:
            fastavro.reader(fo)
        assert False, 'opened non avro file'
    except ValueError:
        pass
