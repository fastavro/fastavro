import fastavro

from fastavro.six import MemoryIO

import pytest

pytestmark = pytest.mark.usefixtures("clean_schemas")


def test_schemaless_writer_and_reader():
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{
            "name": "field",
            "type": {"type": "string"}
        }]
    }
    record = {"field": "test"}
    new_file = MemoryIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, schema)
    assert record == new_record


def test_schemaless_writer_and_reader_with_union():
    """Testing basic functionality of reader with union when option to
    return_record_name is true.
    """
    schema = {
        "name": "Message",
        "type": "record",
        "namespace": "test",
        "fields": [
            {"name": "id",
             "type": "long"},
            {"name": "payload",
             "type": [
                 {
                     "name": "ApplicationCreated",
                     "type": "record",
                     "fields": [
                         {"name": "applicationId", "type": "string"},
                         {"name": "data", "type": "string"}
                     ]
                 },
                 {
                     "name": "ApplicationSubmitted",
                     "type": "record",
                     "fields": [
                         {"name": "applicationId", "type": "string"},
                         {"name": "data", "type": "string"}
                     ]
                 },
             ]}
        ]
    }
    record = {"id": 123, "payload": (
        "test.ApplicationSubmitted", {"applicationId": "123456789UT",
                                      "data": "..."})}
    new_file = MemoryIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(
        new_file, schema, return_record_name=True
    )
    assert record == new_record


def test_boolean_roundtrip():
    schema = {
        "type": "record",
        "name": "test_boolean_roundtrip",
        "fields": [{
            "name": "field",
            "type": "boolean"
        }]
    }
    record = {"field": True}
    new_file = MemoryIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, schema)
    assert record == new_record

    record = {"field": False}
    new_file = MemoryIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, schema)
    assert record == new_record


def test_default_values_in_reader():
    writer_schema = {
        'name': 'name1',
        'type': 'record',
        'namespace': 'namespace1',
        'fields': [{
            'doc': 'test',
            'type': 'int',
            'name': 'good_field'
        }],
        'doc': 'test'
    }

    reader_schema = {
        'name': 'name1',
        'doc': 'test',
        'namespace': 'namespace1',
        'fields': [{
            'name': 'good_field',
            'doc': 'test',
            'type': 'int'
        }, {
            'name': 'good_compatible_field',
            'doc': 'test',
            'default': 1,
            'type': 'int'
        }],
        'type': 'record'
    }

    record = {'good_field': 1}
    new_file = MemoryIO()
    fastavro.schemaless_writer(new_file, writer_schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(
        new_file,
        writer_schema,
        reader_schema,
    )
    assert new_record == {'good_field': 1, 'good_compatible_field': 1}
