from ._read_py import reader
from .io.json_decoder import AvroJSONDecoder


def json_reader(fo, schema):
    """Iterator over records in an avro json file.

    Parameters
    ----------
    fo: file-like
        Input stream
    reader_schema: dict
        Reader schema


    Example::

        from fastavro import json_reader

        schema = {
            'doc': 'A weather reading.',
            'name': 'Weather',
            'namespace': 'test',
            'type': 'record',
            'fields': [
                {'name': 'station', 'type': 'string'},
                {'name': 'time', 'type': 'long'},
                {'name': 'temp', 'type': 'int'},
            ]
        }

        with open('some-file', 'r') as fo:
            avro_reader = json_reader(fo, schema)
            for record in avro_reader:
                print(record)
    """
    return reader(AvroJSONDecoder(fo), schema)
