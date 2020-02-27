import pytest
import fastavro
from fastavro.schema import (
    SchemaParseException, UnknownType, parse_schema, fullname, expand_schema
)

pytestmark = pytest.mark.usefixtures("clean_schemas")


def test_named_types_have_names():
    record_schema = {
        "type": "record",
        "fields": [{
            "name": "field",
            "type": "string",
        }],
    }

    with pytest.raises(SchemaParseException):
        fastavro.parse_schema(record_schema)

    error_schema = {
        "type": "error",
        "fields": [{
            "name": "field",
            "type": "string",
        }],
    }

    with pytest.raises(SchemaParseException):
        fastavro.parse_schema(error_schema)

    fixed_schema = {
        "type": "fixed",
        "size": 1,
    }

    with pytest.raises(SchemaParseException):
        fastavro.parse_schema(fixed_schema)

    enum_schema = {
        "type": "enum",
        "symbols": ["FOO"],
    }

    with pytest.raises(SchemaParseException):
        fastavro.parse_schema(enum_schema)

    # Should parse with name
    for schema in (record_schema, error_schema, fixed_schema, enum_schema):
        schema["name"] = "test_named_types_have_names"
        fastavro.parse_schema(schema)


def test_parse_schema():
    schema = {
        "type": "record",
        "name": "test_parse_schema",
        "fields": [{
            "name": "field",
            "type": "string",
        }],
    }

    parsed_schema = parse_schema(schema)
    assert "__fastavro_parsed" in parsed_schema

    parsed_schema_again = parse_schema(parsed_schema)
    assert parsed_schema_again == parsed_schema


def test_unknown_type():
    schema = {
        "type": "unknown",
    }

    with pytest.raises(UnknownType):
        parse_schema(schema)


def test_aliases_are_preserved():
    schema = {
        "type": "record",
        "name": "test_parse_schema",
        "fields": [{
            "name": "field",
            "type": "string",
            "aliases": ["test"],
        }],
    }

    parsed_schema = parse_schema(schema)
    assert "aliases" in parsed_schema["fields"][0]


def test_aliases_is_a_list():
    """https://github.com/fastavro/fastavro/issues/206"""
    schema = {
        "type": "record",
        "name": "test_parse_schema",
        "fields": [{
            "name": "field",
            "type": "string",
            "aliases": "foobar",
        }],
    }

    with pytest.raises(SchemaParseException):
        parse_schema(schema)


def test_scale_is_an_int():
    """https://github.com/fastavro/fastavro/issues/262"""
    schema = {
        "type": "record",
        "name": "test_scale_is_an_int",
        "fields": [{
            "name": "field",
            "type": {
                "logicalType": "decimal",
                "precision": 5,
                "scale": "2",
                "type": "bytes",
            },
        }],
    }

    with pytest.raises(
        SchemaParseException, match="decimal scale must be a postive integer"
    ):
        parse_schema(schema)


def test_precision_is_an_int():
    """https://github.com/fastavro/fastavro/issues/262"""
    schema = {
        "type": "record",
        "name": "test_scale_is_an_int",
        "fields": [{
            "name": "field",
            "type": {
                "logicalType": "decimal",
                "precision": "5",
                "scale": 2,
                "type": "bytes",
            },
        }],
    }

    with pytest.raises(
        SchemaParseException,
        match="decimal precision must be a postive integer",
    ):
        parse_schema(schema)


def test_named_type_cannot_be_redefined():
    schema = {
        "type": "record",
        "namespace": "test.avro.training",
        "name": "SomeMessage",
        "fields": [{
            "name": "is_error",
            "type": "boolean",
            "default": False,
        }, {
            "name": "outcome",
            "type": [{
                "type": "record",
                "name": "SomeMessage",
                "fields": [],
            }, {
                "type": "record",
                "name": "ErrorRecord",
                "fields": [{
                    "name": "errors",
                    "type": {"type": "map", "values": "string"},
                    "doc": "doc",
                }],
            }],
        }],
    }

    with pytest.raises(
        SchemaParseException,
        match="redefined named type: test.avro.training.SomeMessage",
    ):
        parse_schema(schema)

    schema = {
        "type": "record",
        "name": "SomeMessage",
        "fields": [{
            "name": "field1",
            "type": {
                "type": "record",
                "name": "ThisName",
                "fields": [],
            },
        }, {
            "name": "field2",
            "type": {
                "type": "enum",
                "name": "ThisName",
                "symbols": ["FOO", "BAR"],
            },
        }],
    }

    with pytest.raises(
        SchemaParseException, match="redefined named type: ThisName"
    ):
        parse_schema(schema)

    schema = {
        "type": "record",
        "name": "SomeMessage",
        "fields": [{
            "name": "field1",
            "type": {
                "type": "record",
                "name": "ThatName",
                "fields": [],
            },
        }, {
            "name": "field2",
            "type": {
                "type": "fixed",
                "name": "ThatName",
                "size": 8,
            },
        }],
    }

    with pytest.raises(
        SchemaParseException, match="redefined named type: ThatName"
    ):
        parse_schema(schema)


def test_doc_left_in_parse_schema():
    schema = {
        "type": "record",
        "name": "test_doc_left_in_parse_schema",
        "doc": "blah",
        "fields": [
            {
                "name": "field1",
                "type": "string",
                "default": ""
            }
        ]
    }
    assert schema == parse_schema(schema, _write_hint=False)


def test_schema_fullname_api():
    schema = {
        "type": "record",
        "namespace": "namespace",
        "name": "test_schema_fullname_api",
        "fields": [],
    }

    assert fullname(schema) == "namespace.test_schema_fullname_api"


def test_schema_expansion():
    """https://github.com/fastavro/fastavro/issues/314"""
    sub_schema = {
        "name": "Dependency",
        "namespace": "com.namespace.dependencies",
        "type": "record",
        "fields": [
            {"name": "sub_field_1", "type": "string"}
        ]
    }

    outer_schema = {
        "name": "MasterSchema",
        "namespace": "com.namespace.master",
        "type": "record",
        "fields": [{
            "name": "field_1",
            "type": "com.namespace.dependencies.Dependency"
        }]
    }

    combined = {
        "name": "com.namespace.master.MasterSchema",
        "type": "record",
        "fields": [{"name": "field_1", "type": {
                "name": "com.namespace.dependencies.Dependency",
                "type": "record",
                "fields": [
                    {"name": "sub_field_1", "type": "string"}
                ]
            }
        }]
    }

    expand_schema(sub_schema)
    parsed = expand_schema(outer_schema)

    assert parsed == combined


def test_schema_expansion_2():
    """https://github.com/fastavro/fastavro/issues/314"""
    original_schema = {
        "name": "MasterSchema",
        "namespace": "com.namespace.master",
        "type": "record",
        "fields": [{
            "name": "field_1",
            "type": {
                "name": "Dependency",
                "namespace": "com.namespace.dependencies",
                "type": "record",
                "fields": [
                    {"name": "sub_field_1", "type": "string"}
                ]
            }
        }, {
            "name": "field_2",
            "type": "com.namespace.dependencies.Dependency"
        }]
    }

    expanded_schema = {
        "name": "com.namespace.master.MasterSchema",
        "type": "record",
        "fields": [{
            "name": "field_1",
            "type": {
                "name": "com.namespace.dependencies.Dependency",
                "type": "record",
                "fields": [
                    {"name": "sub_field_1", "type": "string"}
                ]
            }
        }, {
            "name": "field_2",
            "type": {
                "name": "com.namespace.dependencies.Dependency",
                "type": "record",
                "fields": [
                    {"name": "sub_field_1", "type": "string"}
                ]
            }
        }]
    }

    assert expanded_schema == expand_schema(original_schema)


def test_expanding_recursive_schemas_should_stop():
    """https://github.com/fastavro/fastavro/issues/314"""
    sub_schema = {
        "name": "LongList",
        "type": "record",
        "fields": [
            {"name": "value", "type": "long"},
            {"name": "next", "type": ["LongList", "null"]},
        ]
    }

    parsed = expand_schema(sub_schema)
    assert sub_schema == parsed
