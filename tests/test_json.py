from fastavro import json_writer, json_reader
from fastavro.six import MemoryIO

import pytest

pytestmark = pytest.mark.usefixtures("clean_readers_writers_and_schemas")


def roundtrip(schema, records):
    new_file = MemoryIO()
    json_writer.writer(new_file, schema, records)
    new_file.seek(0)

    new_records = list(json_reader.reader(new_file, schema))
    return new_records


def test_json():
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{
            "name": "null",
            "type": "null",
        }, {
            "name": "boolean",
            "type": "boolean",
        }, {
            "name": "string",
            "type": "string",
        }, {
            "name": "bytes",
            "type": "bytes",
        }, {
            "name": "int",
            "type": "int",
        }, {
            "name": "long",
            "type": "long",
        }, {
            "name": "float",
            "type": "float",
        }, {
            "name": "double",
            "type": "double",
        }, {
            "name": "fixed",
            "type": {
                "type": "fixed",
                "name": "fixed_field",
                "size": 5,
            },
        }, {
            "name": "union",
            "type": [
                'null',
                'int',
                {
                    "type": "record",
                    "name": "union_record",
                    "fields": [{
                        "name": "union_record_field",
                        "type": "string",
                    }],
                },
            ]
        }, {
            "name": "enum",
            "type": {
                "type": "enum",
                "symbols": ["FOO", "BAR"],
            },
        }, {
            "name": "array",
            "type": {
                "type": "array",
                "items": "string",
            },
        }, {
            "name": "map",
            "type": {
                "type": "map",
                "values": "int",
            },
        }, {
            "name": "record",
            "type": {
                "type": "record",
                "name": "subrecord",
                "fields": [{
                    "name": "sub_int",
                    "type": "int",
                }],
            },
        }]
    }

    records = [{
        'null': None,
        'boolean': True,
        'string': 'foo',
        'bytes': b'\xe2\x99\xa5',
        'int': 1,
        'long': 1 << 33,
        'float': 2.2,
        'double': 3.3,
        'fixed': b'\x61\x61\x61\x61\x61',
        'union': None,
        'enum': 'BAR',
        'array': ['a', 'b'],
        'map': {
            'c': 1,
            'd': 2
        },
        'record': {
            'sub_int': 123,
        }
    }, {
        'null': None,
        'boolean': True,
        'string': 'foo',
        'bytes': b'\xe2\x99\xa5',
        'int': 1,
        'long': 1 << 33,
        'float': 2.2,
        'double': 3.3,
        'fixed': b'\x61\x61\x61\x61\x61',
        'union': 321,
        'enum': 'BAR',
        'array': ['a', 'b'],
        'map': {
            'c': 1,
            'd': 2
        },
        'record': {
            'sub_int': 123,
        }
    }, {
        'null': None,
        'boolean': True,
        'string': 'foo',
        'bytes': b'\xe2\x99\xa5',
        'int': 1,
        'long': 1 << 33,
        'float': 2.2,
        'double': 3.3,
        'fixed': b'\x61\x61\x61\x61\x61',
        'union': {
            'union_record_field': 'union_field',
        },
        'enum': 'BAR',
        'array': ['a', 'b'],
        'map': {
            'c': 1,
            'd': 2
        },
        'record': {
            'sub_int': 123,
        }
    }]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_more_than_one_record():
    schema = {
        "type": "record",
        "name": "test_more_than_one_record",
        "namespace": "test",
        "fields": [{
            "name": "string",
            "type": "string",
        }, {
            "name": "int",
            "type": "int",
        }]
    }

    records = [{
        'string': 'foo',
        'int': 1,
    }, {
        'string': 'bar',
        'int': 2,
    }]

    new_records = roundtrip(schema, records)
    assert records == new_records
