import fastavro
from io import BytesIO

from fastavro import writer as fastavro_writer
from fastavro.reader import SchemaResolutionError
from nose.tools import raises

schema_dict_a = {
    "namespace": "example.avro2",
    "type": "record",
    "name": "evtest",
    "fields": [
        {"name": "a", "type": "int"}
    ]
}

record_a = {"a": 123}

schema_dict_a_b = {
    "namespace": "example.avro2",
    "type": "record",
    "name": "evtest",
    "fields": [
        {"name": "a", "type": "int"},
        {"name": "b", "type": ["null", "int"], "default": None}
    ]
}

record_a_b = {"a": 234, "b": 345}

schema_dict_a_c = {
    "namespace": "example.avro2",
    "type": "record",
    "name": "evtest",
    "fields": [
        {"name": "a", "type": "int"},
        {"name": "c", "type": ["null", "int"]}
    ]
}


def avro_to_bytes_with_schema(avro_schema, avro_dict):
    with BytesIO() as bytes_io:
        fastavro_writer(bytes_io, avro_schema, [avro_dict])
        return bytes_io.getvalue()


def bytes_with_schema_to_avro(avro_read_schema, binary):
    with BytesIO(binary) as bytes_io:
        reader = fastavro.reader(bytes_io, avro_read_schema)
        return next(reader)


def test_evolution_drop_field():
    record_bytes_a_b = avro_to_bytes_with_schema(schema_dict_a_b, record_a_b)
    record_a = bytes_with_schema_to_avro(schema_dict_a, record_bytes_a_b)
    assert "b" not in record_a


def test_evolution_add_field_with_default():
    record_bytes_a = avro_to_bytes_with_schema(schema_dict_a, record_a)
    record_b = bytes_with_schema_to_avro(schema_dict_a_b, record_bytes_a)
    assert "b" in record_b
    assert record_b.get("b") is None


@raises(SchemaResolutionError)
def test_evolution_add_field_without_default():
    record_bytes_a = avro_to_bytes_with_schema(schema_dict_a, record_a)
    bytes_with_schema_to_avro(schema_dict_a_c, record_bytes_a)
