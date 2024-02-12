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
        "name": "test_aliases_not_present",
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
        "name": "test_incompatible_aliases",
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
        "name": "test_aliases_in_reader_schema",
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


def test_record_name_alias():
    schema = {
        "type": "record",
        "name": "test_record_name_alias",
        "fields": [{"name": "test", "type": "int"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_record_name_alias_new",
        "aliases": "test_record_name_alias",
        "fields": [{"name": "test", "type": "int"}],
    }

    records = [{"test": 1}]

    assert roundtrip(schema, records, new_schema) == [{"test": 1}]


def test_fixed_name_alias():
    schema = {"type": "fixed", "name": "test_fixed_name_alias", "size": 4}

    new_schema = {
        "type": "fixed",
        "name": "test_fixed_name_alias_new",
        "aliases": "test_fixed_name_alias",
        "size": 4,
    }

    records = [b"1234"]

    assert roundtrip(schema, records, new_schema) == [b"1234"]


def test_enum_name_alias():
    schema = {"type": "enum", "name": "test_enum_name_alias", "symbols": ["FOO"]}

    new_schema = {
        "type": "enum",
        "name": "test_enum_name_alias_new",
        "aliases": "test_enum_name_alias",
        "symbols": ["FOO"],
    }

    records = ["FOO"]

    assert roundtrip(schema, records, new_schema) == ["FOO"]


def test_alias_in_same_namespace():
    """https://github.com/fastavro/fastavro/issues/648"""

    # Old schema that matches the input_json
    old_schema = fastavro.parse_schema(
        {
            "type": "record",
            "namespace": "com.node40",
            "name": "generated",
            "fields": [
                {"name": "key1", "type": "string"},
                {"name": "key2", "type": "string"},
                {"name": "key3", "type": "string"},
            ],
        }
    )

    # New schema with old schema names as aliases
    new_schema = fastavro.parse_schema(
        {
            "type": "record",
            "namespace": "com.node40",
            "name": "test",
            "aliases": ["generated"],
            "fields": [
                {"name": "k1", "type": "string", "aliases": ["key1"]},
                {"name": "k2", "type": "string", "aliases": ["key2"]},
                {"name": "k3", "type": "string", "aliases": ["key3"]},
            ],
        }
    )

    # Sample data
    input_json = [{"key1": "value1", "key2": "value2", "key3": "value3"}]

    # Write to buffer as serialized avro using old_schema
    buffer = BytesIO()
    fastavro.writer(buffer, old_schema, input_json, validator=True)
    buffer.seek(0)

    read_messages = list(fastavro.reader(buffer, new_schema))
    expected_messages = [{"k1": "value1", "k2": "value2", "k3": "value3"}]
    assert read_messages == expected_messages


def test_alias_in_different_namespace():
    # Old schema that matches the input_json
    old_schema = fastavro.parse_schema(
        {
            "type": "record",
            "namespace": "the.old.namespace",
            "name": "old_name",
            "fields": [{"name": "key1", "type": "string"}],
        }
    )

    # New schema with old schema names as aliases
    new_schema = fastavro.parse_schema(
        {
            "type": "record",
            "namespace": "the.new.namespace",
            "name": "new_name",
            "aliases": ["the.old.namespace.old_name"],
            "fields": [{"name": "k1", "type": "string", "aliases": ["key1"]}],
        }
    )

    # Sample data
    input_json = [{"key1": "foo"}]

    # Write to buffer as serialized avro using old_schema
    buffer = BytesIO()
    fastavro.writer(buffer, old_schema, input_json, validator=True)
    buffer.seek(0)

    read_messages = list(fastavro.reader(buffer, new_schema))
    expected_messages = [
        {
            "k1": "foo",
        }
    ]
    assert read_messages == expected_messages
