'''Fast Avro file iteration.

Most of the code here is ripped off the Python avro package. It's missing a lot
of features in order to get speed.

The only onterface function is iter_avro, example usage::

    # Reading
    import fastavro as avro

    with open('some-file.avro', 'rb') as fo:
        reader = fastavro.reader(fo)
        schema = reader.schema

        for record in reader:
            process_record(record)


    # Writing
    from fastavro import writer

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

    records = [
        {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
        {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
        {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
        {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
    ]

    with open('weather.avro', 'wb') as out:
        writer(out, schema, records)
'''

__version__ = '0.9.5'


try:
    from . import _reader
    from . import _writer
    from . import _schema
except ImportError as e:
    from . import reader as _reader
    from . import writer as _writer
    from . import schema as _schema


def _acquaint_schema(schema):
    _reader.acquaint_schema(schema)
    _writer.acquaint_schema(schema)

reader = iter_avro = _reader.iter_avro
schemaless_reader = _reader.schemaless_reader
load = _reader.read_data
writer = _writer.writer
schemaless_writer = _writer.schemaless_writer
dump = _writer.write_data
acquaint_schema = _acquaint_schema
_schema.acquaint_schema = _acquaint_schema

__all__ = [
    n for n in locals().keys() if not n.startswith('_')
] + ['__version__']
