import fastavro

import pytest

from io import BytesIO
import sys


schema = {
  "fields": [
    {
      "name": "str_null",
      "type": ["null", "string"]
    },
    {
      "name": "str",
      "type": "string"
    },
    {
      "name": "integ_null",
      "type": ["null", "int"]
    },
    {
      "name": "integ",
      "type": "int"
    }
  ],
  "namespace": "namespace",
  "name": "missingerror",
  "type": "record"
}


def serialize(schema, *records):
    buffer = BytesIO()
    fastavro.writer(buffer, schema, records)
    serialized = buffer.getvalue()
    return serialized


def test_types_match():
    records = [{
        'str_null': 'str',
        'str': 'str',
        'integ_null': 21,
        'integ': 21,
    }]
    serialize(schema, *records)


def test_string_in_int_raises():
    records = [{
        'str_null': 'str',
        'str': 'str',
        'integ_null': 'str',
        'integ': 21,
    }]

    with pytest.raises((TypeError, ValueError)):
        serialize(schema, *records)


def test_string_in_int_null_raises():
    records = [{
        'str_null': 'str',
        'str': 'str',
        'integ_null': 11,
        'integ': 'str',
    }]
    with pytest.raises((TypeError, ValueError)):
        serialize(schema, *records)


def test_int_in_string_null_raises():
    records = [{
        'str_null': 11,
        'str': 'str',
        'integ_null': 21,
        'integ': 21,
    }]
    with pytest.raises((TypeError, ValueError)):
        serialize(schema, *records)


if sys.version_info[:2] == (3, 4):
    @pytest.mark.skip(reason='FIXME: this fails on 3.4')
    def test_int_in_string_raises():
        pass
else:
    def test_int_in_string_raises():
        records = [{
            'str_null': 'str',
            'str': 11,
            'integ_null': 21,
            'integ': 21,
        }]

        with pytest.raises((TypeError, ValueError, AttributeError)):
            serialize(schema, *records)
