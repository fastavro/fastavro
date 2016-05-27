import fastavro
from fastavro.exceptions import AvroValueError

from fastavro.six import MemoryIO


def assert_error(schema, records, message_substring):
    fo = MemoryIO()
    try:
        fastavro.writer(fo, schema, records)
    except AvroValueError as e:
        assert message_substring in str(e)
    else:
        assert False, 'AvroValueError should have been raised'


def test_correct_record_indexing():
    schema = {
        "type": "record",
        "name": "test_record",
        "fields": [{
            "name": "test",
            "type": "string",
        }]
    }

    records = [{'test': None}]
    assert_error(schema, records, '<record[0]>')

    records = [{'test': 'a'}, {'test': None}]
    assert_error(schema, records, '<record[1]>')


def test_correct_field_detection():
    schema = {
        "type": "record",
        "name": "test_record",
        "fields": [{
            "name": "A",
            "type": "string",
        }, {
            "name": "B",
            "type": "string",
        }, {
            "name": "C",
            "type": "string",
        }]
    }

    records = [{'A': None, 'B': 'foo', 'C': 'foo'}]
    assert_error(schema, records, '<field[A]>')

    records = [{'A': 'foo', 'B': None, 'C': 'foo'}]
    assert_error(schema, records, '<field[B]>')


def test_correct_array_indexing():
    schema = {
        "type": "record",
        "name": "test_record",
        "fields": [{
            "name": "foo",
            "type": {
                "type": "array",
                "items": "int",
            },
        }]
    }

    records = [{'foo': [None, 1, 2]}]
    assert_error(schema, records, '<array[0]>')

    records = [{'foo': [0, None, 2]}]
    assert_error(schema, records, '<array[1]>')


def test_correct_map_indexing():
    schema = {
        "type": "record",
        "name": "test_record",
        "fields": [{
            "name": "foo",
            "type": {
                "type": "map",
                "values": "int",
            },
        }]
    }

    # Invalid map key
    records = [{'foo': {'key': 0, None: 1}}]
    assert_error(schema, records, '<map_key[None]>')

    # Invalide map values
    records = [{'foo': {'key': None, 'key2': 1}}]
    assert_error(schema, records, '<map_value[key]>')

    records = [{'foo': {'key': 0, 'key2': None}}]
    assert_error(schema, records, '<map_value[key2]>')
