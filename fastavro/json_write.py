from ._write_py import writer
from .io.json_encoder import AvroJSONEncoder


def json_writer(fo, schema, records):
    return writer(AvroJSONEncoder(fo), schema, records)
