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
            "name": "array_decimal",
            "type": ["null", {"type": "array",
                              "items": {"type": "bytes",
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
        'array_decimal': [Decimal("123.456")],
        'multi_union_time': datetime.datetime.now(),
        'array_record': [dict(f1="1", f2=Decimal("123.456"))]
    }
    binary = serialize(schema, data1)
    data2 = deserialize(schema, binary)
    assert (data1 == data2)
