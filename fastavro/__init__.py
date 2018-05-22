'''Fast Avro file iteration.

Example usage::

    # Reading
    import fastavro

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

    # 'records' can be an iterable (including generator)
    records = [
        {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
        {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
        {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
        {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
    ]

    with open('weather.avro', 'wb') as out:
        writer(out, schema, records)
'''

__version_info__ = (0, 19, 4)
__version__ = '%s.%s.%s' % __version_info__


import fastavro.read
import fastavro.write
import fastavro.schema
import fastavro.validation


def _acquaint_schema(schema):
    """Add a new schema to the schema repo.

    Parameters
    ----------
    schema: dict
        Schema to add to repo
    """
    fastavro.read.acquaint_schema(schema)
    fastavro.write.acquaint_schema(schema)


reader = iter_avro = fastavro.read.reader
schemaless_reader = fastavro.read.schemaless_reader
load = fastavro.read.read_data
writer = fastavro.write.writer
schemaless_writer = fastavro.write.schemaless_writer
dump = fastavro.write.dump
acquaint_schema = _acquaint_schema
fastavro.schema.acquaint_schema = _acquaint_schema
is_avro = fastavro.read.is_avro
validate = fastavro.validation.validate

__all__ = [
    n for n in locals().keys() if not n.startswith('_')
] + ['__version__']
