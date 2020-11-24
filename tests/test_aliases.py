from io import BytesIO
import fastavro

import pytest


def roundtrip(schema, records, new_schema):
    new_file = BytesIO()
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)

    reader = fastavro.reader(new_file, new_schema)
    new_records = list(reader)
    return new_records


def test_aliases_not_present():
    schema = {
        "type": "record",
        "name": "test_aliases_not_present",
        "fields": [{"name": "test", "type": "double"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_aliases_not_present_new",
        "fields": [
            {"name": "newtest", "type": "double", "aliases": ["testX"]},
        ],
    }

    records = [{"test": 1.2}]

    with pytest.raises(fastavro.read.SchemaResolutionError):
        roundtrip(schema, records, new_schema)


def test_incompatible_aliases():
    schema = {
        "type": "record",
        "name": "test_incompatible_aliases",
        "fields": [{"name": "test", "type": "double"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_incompatible_aliases_new",
        "fields": [
            {"name": "newtest", "type": "int", "aliases": ["test"]},
        ],
    }

    records = [{"test": 1.2}]

    with pytest.raises(fastavro.read.SchemaResolutionError):
        roundtrip(schema, records, new_schema)


def test_aliases_in_reader_schema():
    schema = {
        "type": "record",
        "name": "test_aliases_in_reader_schema",
        "fields": [{"name": "test", "type": "int"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_aliases_in_reader_schema_new",
        "fields": [{"name": "newtest", "type": "int", "aliases": ["test"]}],
    }

    records = [{"test": 1}]

    assert roundtrip(schema, records, new_schema) == [{"newtest": 1}]


def test_aliases_with_default_value_and_field_added():
    """https://github.com/fastavro/fastavro/issues/225"""
    schema = {
        "type": "record",
        "name": "test_aliases_with_default_value",
        "fields": [{"name": "test", "type": "int"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_aliases_with_default_value",
        "fields": [
            {"name": "newtest", "type": "int", "default": 0, "aliases": ["test"]},
            {"name": "test2", "type": "int", "default": 100},
        ],
    }

    records = [{"test": 1}]

    new_records = roundtrip(schema, records, new_schema)
    assert new_records == [{"newtest": 1, "test2": 100}]
