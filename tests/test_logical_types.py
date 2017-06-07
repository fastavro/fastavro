import datetime
from decimal import Decimal
from io import BytesIO

from nose.tools import raises

import fastavro
from fastavro.reader import read_bytes_decimal


schema = {
    "fields": [
        {
            "name": "date",
            "type": {'type': 'int', 'logicalType': 'date'}
        },
        {
            "name": "timestamp-millis",
            "type": {'type': 'long', 'logicalType': 'timestamp-millis'}
        },
        {
            "name": "timestamp-micros",
            "type": {'type': 'long', 'logicalType': 'timestamp-micros'}
        },
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


def test_logical_types():
    data1 = {
        'date': datetime.date.today(),
        'timestamp-millis': datetime.datetime.now(),
        'timestamp-micros': datetime.datetime.now(),

    }
    binary = serialize(schema, data1)
    data2 = deserialize(schema, binary)
    assert (data1['date'] == data2['date'])
    assert (data1['timestamp-micros'] == data2['timestamp-micros'])
    assert (int(data1['timestamp-millis'].microsecond / 1000) ==
            int(data2['timestamp-millis'].microsecond))


schema_null = {
    "fields": [
        {
            "name": "date",
            "type": ["null", {'type': 'int', 'logicalType': 'date'}]
        },
    ],
    "namespace": "namespace",
    "name": "name",
    "type": "record"
}


def test_null():
    data1 = {
        # 'date': None,
    }
    binary = serialize(schema_null, data1)
    data2 = deserialize(schema_null, binary)
    assert (data2['date'] is None)


schema_top = {
    "name": "n",
    "namespace": "namespace",
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 15,
    "scale": 3,
}


def test_top():
    data1 = Decimal("123.456")
    binary = serialize(schema_top, data1)

    data2 = deserialize(schema_top, binary)
    assert (data1 == data2)


def test_negative():
    data1 = Decimal("-2.90")
    binary = serialize(schema_top, data1)
    data2 = deserialize(schema_top, binary)
    assert (data1 == data2)


def test_zero():
    data1 = Decimal("0.0")
    binary = serialize(schema_top, data1)
    data2 = deserialize(schema_top, binary)
    assert (data1 == data2)


schema_leftmost = {
    "name": "n",
    "namespace": "namespace",
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 18,
    "scale": 6,
}


def test_leftmost():
    binary = b'\xd5F\x80'
    res = read_bytes_decimal(binary, schema_leftmost)
    assert (Decimal("-2.80") == res)


@raises(AssertionError)
def test_scale():
    data1 = Decimal("123.456678")  # does not fit scale
    serialize(schema_top, data1)
