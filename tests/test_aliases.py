import fastavro
from fastavro.six import MemoryIO

import pytest


def test_aliases_not_present():
    schema = {
        "type": "record",
        "name": "test_aliases_not_present",
        "fields": [{
            "name": "test",
            "type": "double"
        }]
    }

    new_schema = {
        "type": "record",
        "name": "test_aliases_not_present_new",
        "fields": [
            {"name": "newtest", "type": "double", "aliases": ["testX"]},
        ]
    }

    new_file = MemoryIO()
    records = [{"test": 1.2}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    reader = fastavro.reader(new_file, new_schema)
    with pytest.raises(fastavro.read.SchemaResolutionError):
        list(reader)


def test_incompatible_aliases():
    schema = {
        "type": "record",
        "name": "test_incompatible_aliases",
        "fields": [{
            "name": "test",
            "type": "double"
        }]
    }

    new_schema = {
        "type": "record",
        "name": "test_incompatible_aliases_new",
        "fields": [
            {"name": "newtest", "type": "int", "aliases": ["test"]},
        ]
    }

    new_file = MemoryIO()
    records = [{"test": 1.2}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    reader = fastavro.reader(new_file, new_schema)
    with pytest.raises(fastavro.read.SchemaResolutionError):
        list(reader)


def test_aliases_in_reader_schema():
    schema = {
        "type": "record",
        "name": "test_aliases_in_reader_schema",
        "fields": [{
            "name": "test",
            "type": "int"
        }]
    }

    new_schema = {
        "type": "record",
        "name": "test_aliases_in_reader_schema_new",
        "fields": [{
            "name": "newtest",
            "type": "int",
            "aliases": ["test"]
        }]
    }

    new_file = MemoryIO()
    records = [{"test": 1}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records[0]["newtest"] == records[0]["test"]
