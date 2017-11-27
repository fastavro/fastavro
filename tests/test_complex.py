import datetime
from decimal import Decimal
from io import BytesIO

import fastavro

schema = {
    "fields": [
        {
            "name": "array_string",
            "type": {"type": "array", "items": "string"}
        },
        {
            "name": "multi_union_time",
            "type": ["null", "string", {"type": "long",
                                        "logicalType": "timestamp-micros"}]
        },
        {
            "name": "array_bytes_decimal",
            "type": ["null", {"type": "array",
                              "items": {"type": "bytes",
                                        "logicalType": "decimal",
                                        "precision": 18,
                                        "scale": 6, }
                              }]
        },
        {
            "name": "array_fixed_decimal",
            "type": ["null", {"type": "array",
                              "items": {"type": "fixed",
                                        "size": 8,
                                        "logicalType": "decimal",
                                        "precision": 18,
                                        "scale": 6, }
                              }]
        },
        {
            "name": "array_record",
            "type": {"type": "array", "items": {
                "type": "record",
                "fields": [
                    {
                        "name": "f1",
                        "type": "string"
                    },
                    {
                        "name": "f2",
                        "type": {"type": "bytes",
                                 "logicalType": "decimal",
                                 "precision": 18,
                                 "scale": 6, }

                    }
                ]
            }
                     }
        }
    ],
    "namespace": "namespace",
    "name": "name",
    "type": "record"
}


def serialize(schema, data):
    bytes_writer = BytesIO()
    fastavro.schemaless_writer(bytes_writer, schema, data)
    return bytes_writer.getvalue()


def deserialize(schema, binary):
    bytes_writer = BytesIO()
    bytes_writer.write(binary)
    bytes_writer.seek(0)

    res = fastavro.schemaless_reader(bytes_writer, schema)
    return res


def test_complex_schema():
    data1 = {
        'array_string': ['a', "b", "c"],
        'multi_union_time': datetime.datetime.now(),
        'array_bytes_decimal': [Decimal("123.456")],
        'array_fixed_decimal': [Decimal("123.456")],
        'array_record': [{'f1': '1', 'f2': Decimal("123.456")}]
    }
    binary = serialize(schema, data1)
    data2 = deserialize(schema, binary)
    assert (data1 == data2)


def test_complex_schema_nulls():
    data1 = {
        'array_string': ['a', "b", "c"],
        'array_record': [{'f1': '1', 'f2': Decimal("123.456")}]
    }
    binary = serialize(schema, data1)
    data2 = deserialize(schema, binary)
    data1_compare = data1
    data1_compare.update(
        {'multi_union_time': None, 'array_bytes_decimal': None,
         'array_fixed_decimal': None})
    assert (data1_compare == data2)
