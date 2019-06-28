from ._write_py import writer
from .io.json_encoder import AvroJSONEncoder


def json_writer(fo, schema, records):
    """Write records to fo (stream) according to schema

    Parameters
    ----------
    fo: file-like
        Output stream
    schema: dict
        Writer schema
    records: iterable
        Records to write. This is commonly a list of the dictionary
        representation of the records, but it can be any iterable


    Example::

        from fastavro import json_writer, parse_schema

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
        parsed_schema = parse_schema(schema)

        records = [
            {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
            {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
            {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
            {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
        ]

        with open('some-file', 'w') as out:
            json_writer(out, parsed_schema, records)
    """
    return writer(AvroJSONEncoder(fo), schema, records)
