import fastavro
from fastavro.__main__ import _clean_json_record
import pytest

from decimal import Decimal
from io import BytesIO
from uuid import uuid4
import datetime
import sys
import os

from .conftest import assert_naive_datetime_equal_to_tz_datetime


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
        {
            "name": "uuid",
            "type": {'type': 'string', 'logicalType': 'uuid'}
        },
        {
            "name": "time-millis",
            "type": {'type': 'int', 'logicalType': 'time-millis'}
        },
        {
            "name": "time-micros",
            "type": {'type': 'long', 'logicalType': 'time-micros'}
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


def test_logical_types():
    data1 = {
        'date': datetime.date.today(),
        'timestamp-millis': datetime.datetime.now(),
        'timestamp-micros': datetime.datetime.now(),
        'uuid': uuid4(),
        'time-millis': datetime.datetime.now().time(),
        'time-micros': datetime.datetime.now().time(),

    }
    binary = serialize(schema, data1)
    data2 = deserialize(schema, binary)
    assert (data1['date'] == data2['date'])
    assert_naive_datetime_equal_to_tz_datetime(
        data1['timestamp-micros'],
        data2['timestamp-micros'],
    )
    assert (int(data1['timestamp-millis'].microsecond / 1000) * 1000
            == data2['timestamp-millis'].microsecond)
    assert (int(data1['time-millis'].microsecond / 1000) * 1000
            == data2['time-millis'].microsecond)


@pytest.mark.skipif(os.name == 'nt' and sys.version_info[:2] == (3, 6),
                    reason='Python Bug: https://bugs.python.org/issue29097')
def test_not_logical_ints():
    data1 = {
        'date': 1,
        'timestamp-millis': 2,
        'timestamp-micros': 3,
        'uuid': uuid4(),
        'time-millis': 4,
        'time-micros': 5,

    }
    binary = serialize(schema, data1)
    data2 = deserialize(schema, binary)
    # 1 day since 1970-1-1
    assert (data2['date'] == datetime.date(1970, 1, 2))


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


def test_not_null_datetime():
    data1 = {'date': datetime.datetime(2017, 1, 1)}
    binary = serialize(schema_null, data1)
    data2 = deserialize(schema_null, binary)
    assert (data2['date'] == datetime.date(2017, 1, 1))


def test_not_null_date():
    data1 = {'date': datetime.date(2017, 1, 1)}
    binary = serialize(schema_null, data1)
    data2 = deserialize(schema_null, binary)
    assert (data2['date'] == datetime.date(2017, 1, 1))


# test bytes decimal
schema_bytes_decimal = {
    "name": "n",
    "namespace": "namespace",
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 15,
    "scale": 3,
}


def test_bytes_decimal_negative():
    data1 = Decimal("-2.90")
    binary = serialize(schema_bytes_decimal, data1)
    data2 = deserialize(schema_bytes_decimal, binary)
    assert (data1 == data2)


def test_bytes_decimal_zero():
    data1 = Decimal("0.0")
    binary = serialize(schema_bytes_decimal, data1)
    data2 = deserialize(schema_bytes_decimal, binary)
    assert (data1 == data2)


def test_bytes_decimal_positive():
    data1 = Decimal("123.456")
    binary = serialize(schema_bytes_decimal, data1)
    data2 = deserialize(schema_bytes_decimal, binary)
    assert (data1 == data2)


def test_bytes_decimal_scale():
    data1 = Decimal("123.456678")  # does not fit scale
    with pytest.raises(ValueError):
        serialize(schema_bytes_decimal, data1)


schema_bytes_decimal_leftmost = {
    "name": "n",
    "namespace": "namespace",
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 18,
    "scale": 6,
}


def test_bytes_decimal_leftmost():
    binary = serialize(schema_bytes_decimal_leftmost, b'\xd5F\x80')
    data2 = deserialize(schema_bytes_decimal_leftmost, binary)
    assert (Decimal("-2.80") == data2)


# test fixed decimal
schema_fixed_decimal = {
    "name": "n",
    "namespace": "namespace",
    "type": "fixed",
    "size": 8,
    "logicalType": "decimal",
    "precision": 15,
    "scale": 3,
}


def test_fixed_decimal_negative():
    data1 = Decimal("-2.90")
    binary = serialize(schema_fixed_decimal, data1)
    data2 = deserialize(schema_fixed_decimal, binary)
    assert (data1 == data2)
    assert (len(binary) == schema_fixed_decimal['size'])


def test_fixed_decimal_zero():
    data1 = Decimal("0.0")
    binary = serialize(schema_fixed_decimal, data1)
    data2 = deserialize(schema_fixed_decimal, binary)
    assert (data1 == data2)
    assert (len(binary) == schema_fixed_decimal['size'])


def test_fixed_decimal_positive():
    data1 = Decimal("123.456")
    binary = serialize(schema_fixed_decimal, data1)
    data2 = deserialize(schema_fixed_decimal, binary)
    assert (data1 == data2)
    assert (len(binary) == schema_fixed_decimal['size'])


def test_fixed_decimal_scale():
    data1 = Decimal("123.456678")  # does not fit scale
    with pytest.raises(ValueError):
        serialize(schema_fixed_decimal, data1)


schema_fixed_decimal_leftmost = {
    "name": "n",
    "namespace": "namespace",
    "type": "fixed",
    "size": 8,
    "logicalType": "decimal",
    "precision": 18,
    "scale": 6,
}


def test_fixed_decimal_binary():
    binary = serialize(schema_fixed_decimal_leftmost,
                       b'\xFF\xFF\xFF\xFF\xFF\xd5F\x80')
    data2 = deserialize(schema_fixed_decimal_leftmost, binary)
    assert (Decimal("-2.80") == data2)


def test_clean_json_list():
    values = [
        datetime.datetime.now(),
        datetime.date.today(),
        uuid4(),
        Decimal('1.23'),
    ]
    str_values = [
        values[0].isoformat(),
        values[1].isoformat(),
        str(values[2]),
        str(values[3]),
    ]
    _clean_json_record(values)
    assert values == str_values


def test_clean_json_dict():
    values = {
        '1': datetime.datetime.now(),
        '2': datetime.date.today(),
        '3': uuid4(),
        '4': Decimal('1.23'),
    }
    str_values = {
        '1': values['1'].isoformat(),
        '2': values['2'].isoformat(),
        '3': str(values['3']),
        '4': str(values['4']),
    }
    _clean_json_record(values)
    assert values == str_values


def test_unknown_logical_type():
    unknown_type_schema = {
        "type": "record",
        "name": "t_customers",
        "fields": [{
            "name": "name",
            "type": {
                "type": "string",
                "logicalType": "varchar",
                "maxLength": 200
            },
        }, {
            "name": "address",
            "type": {
                "type": "record",
                "name": "t_address",
                "fields": [{
                    "name": "street",
                    "type": {
                        "type": "string",
                        "logicalType": "varchar",
                        "maxLength": 240
                    }
                }, {
                    "name": "city",
                    "type": {
                        "type": "string",
                        "logicalType": "varchar",
                        "maxLength": 80
                    }
                }, {
                    "name": "zip",
                    "type": {
                        "type": "string",
                        "logicalType": "varchar",
                        "maxLength": 18
                    }
                }]
            }
        }]
    }

    data1 = {
        'name': 'foo',
        'address': {
            'street': '123 street',
            'city': 'city',
            'zip': '00000'
        }
    }
    converted = serialize(unknown_type_schema, data1)
    data2 = deserialize(unknown_type_schema, converted)
    assert (data1 == data2)
