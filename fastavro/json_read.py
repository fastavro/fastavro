from typing import IO

from ._read_py import reader
from .io.json_decoder import AvroJSONDecoder
from .types import Schema


def json_reader(fo: IO, schema: Schema, *, decoder=AvroJSONDecoder) -> reader:
    """Iterator over records in an avro json file.

    Parameters
    ----------
    fo
        File-like object to read from
    schema
        Reader schema
    decoder
        By default the standard AvroJSONDecoder will be used, but a custom one
        could be passed here


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
    return reader(decoder(fo), schema)
