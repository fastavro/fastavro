'''Fast Avro file iteration.

Most of the code here is ripped off the Python avro package. It's missing a lot
of features in order to get speed.

The only onterface function is iter_avro, example usage::

    import fastavro as avro

    with open('some-file.avro', 'rb') as fo:
        reader = fastavro.reader(fo)
        schema = reader.schema

        for record in reader:
            process_record(record)
'''

__version__ = '0.8.0'


try:
    from . import _reader
    from . import _writer
    from . import _schema
except ImportError as e:
    from . import reader as _reader
    from . import writer as _writer
    from . import schema as _schema

reader = iter_avro = _reader.iter_avro
load = _reader.read_data
writer = _writer.writer
dump = _writer.write_data
acquaint_schema = _schema.acquaint_schema

__all__ = [
    n for n in locals().keys() if not n.startswith('_')
] + ['__version__']
