from fastavro.const import INT_MIN_VALUE, INT_MAX_VALUE, LONG_MIN_VALUE, LONG_MAX_VALUE
from fastavro.validation import ValidationValueError

import fastavro

import pytest

from io import BytesIO

INT_ARRAY = {"type": "array", "items": "int"}
LONG_ARRAY = {"type": "array", "items": "long"}
INT_MAP = {"type": "map", "values": "int"}
LONG_MAP = {"type": "map", "values": "long"}
INT_UNION = ["int"]
LONG_UNION = ["long"]
INT_LONG_UNION = ["int", "long"]
INT_FLOAT_UNION = ["int", "float"]
INT_LONG_FLOAT_DOUBLE_UNION = ["int", "long", "float", "double"]
INT_STRING_UNION = ["int", "string"]
STRING_INT_UNION = ["string", "int"]


@pytest.mark.parametrize(
    "writer_schema,write_data",
    [
        ("int", INT_MIN_VALUE - 1),
        ("int", INT_MAX_VALUE + 1),
        # ("long", LONG_MIN_VALUE - 1),
        # ("long", LONG_MAX_VALUE + 1),
        (INT_LONG_UNION, INT_MIN_VALUE - 1),
        (INT_LONG_UNION, INT_MAX_VALUE + 1),
        (INT_FLOAT_UNION, INT_MIN_VALUE - 1),
        (INT_FLOAT_UNION, INT_MAX_VALUE + 1),
        (INT_LONG_FLOAT_DOUBLE_UNION, INT_MIN_VALUE - 1),
        (INT_LONG_FLOAT_DOUBLE_UNION, INT_MAX_VALUE + 1),
        (INT_LONG_FLOAT_DOUBLE_UNION, LONG_MIN_VALUE - 1),
        (INT_LONG_FLOAT_DOUBLE_UNION, LONG_MAX_VALUE + 1),
    ],
)
def test_writer(writer_schema, write_data):
    bio = BytesIO()
    fastavro.writer(bio, writer_schema, [write_data])


@pytest.mark.parametrize(
    "writer_schema,write_data",
    [
        ("int", INT_MIN_VALUE - 1),
        ("int", INT_MAX_VALUE + 1),
        ("long", LONG_MIN_VALUE - 1),
        ("long", LONG_MAX_VALUE + 1),
    ],
)
def test_writer_with_validator(writer_schema, write_data):
    with pytest.raises(ValidationValueError):
        bio = BytesIO()
        fastavro.writer(bio, writer_schema, [write_data], validator=True)


@pytest.mark.parametrize(
    "writer_schema,write_data",
    [
        (INT_UNION, INT_MIN_VALUE - 1),
        (INT_UNION, INT_MAX_VALUE + 1),
        (LONG_UNION, LONG_MIN_VALUE - 1),
        (LONG_UNION, LONG_MAX_VALUE + 1),
        (INT_LONG_UNION, LONG_MIN_VALUE - 1),
        (INT_LONG_UNION, LONG_MAX_VALUE + 1),
    ],
)
def test_out_of_range_union_write(writer_schema, write_data):
    with pytest.raises(ValidationValueError):
        bio = BytesIO()
        fastavro.writer(bio, writer_schema, [write_data])
