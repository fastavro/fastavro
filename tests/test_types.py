import fastavro

import pytest

from io import BytesIO


schema = {
    "fields": [
        {"name": "str_null", "type": ["null", "string"]},
        {"name": "str", "type": "string"},
        {"name": "integ_null", "type": ["null", "int"]},
        {"name": "integ", "type": "int"},
    ],
    "namespace": "namespace",
    "name": "missingerror",
    "type": "record",
}


def serialize(schema, *records):
    buffer = BytesIO()
    fastavro.writer(buffer, schema, records)
    serialized = buffer.getvalue()
    return serialized


def test_types_match():
    records = [
        {
            "str_null": "str",
            "str": "str",
            "integ_null": 21,
            "integ": 21,
        }
    ]
    serialize(schema, *records)


def test_string_in_int_raises():
    records = [
        {
            "str_null": "str",
            "str": "str",
            "integ_null": "str",
            "integ": 21,
        }
    ]

    with pytest.raises(ValueError):
        serialize(schema, *records)


def test_string_in_int_null_raises():
    records = [
        {
            "str_null": "str",
            "str": "str",
            "integ_null": 11,
            "integ": "str",
        }
    ]
    with pytest.raises(TypeError):
        serialize(schema, *records)


def test_int_in_string_null_raises():
    records = [
        {
            "str_null": 11,
            "str": "str",
            "integ_null": 21,
            "integ": 21,
        }
    ]
    with pytest.raises(ValueError):
        serialize(schema, *records)


def test_int_in_string_raises():
    records = [
        {
            "str_null": "str",
            "str": 11,
            "integ_null": 21,
            "integ": 21,
        }
    ]

    # Raises AttributeError on py2 and TypeError on py3
    with pytest.raises((TypeError, AttributeError)):
        serialize(schema, *records)


@pytest.mark.parametrize(  #
    ("value", "binary"),  #
    [
        (0, b"\x00"),
        (-1, b"\x01"),
        (1, b"\x02"),
        (-2, b"\x03"),
        (2, b"\x04"),
        (2147483647, b"\xfe\xff\xff\xff\x0f"),
        (-2147483648, b"\xff\xff\xff\xff\x0f"),
        (9223372036854775807, b"\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01"),
        (-9223372036854775808, b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01"),
    ],
)
def test_int_binary(value, binary):
    schema = {"type": "long"}
    buffer = BytesIO()

    fastavro.schemaless_writer(buffer, schema, value)
    assert buffer.getvalue() == binary, "Invalid integer encoding."

    deserialized = fastavro.schemaless_reader(BytesIO(binary), schema)
    assert deserialized == value, "Invalid integer decoding."
