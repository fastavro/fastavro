from ._read_py import reader
from .io.json_decoder import AvroJSONDecoder


def json_reader(fo, schema):
    return reader(AvroJSONDecoder(fo), schema)
