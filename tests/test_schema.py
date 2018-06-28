import pytest
import fastavro
from fastavro.schema import SchemaParseException

pytestmark = pytest.mark.usefixtures("clean_readers_writers_and_schemas")


def test_named_types_have_names():
    record_schema = {
        "type": "record",
        "fields": [{
            "name": "field",
            "type": "string",
        }],
    }

    with pytest.raises(SchemaParseException):
        fastavro.acquaint_schema(record_schema)

    error_schema = {
        "type": "error",
        "fields": [{
            "name": "field",
            "type": "string",
        }],
    }

    with pytest.raises(SchemaParseException):
        fastavro.acquaint_schema(error_schema)

    fixed_schema = {
        "type": "fixed",
        "size": 1,
    }

    with pytest.raises(SchemaParseException):
        fastavro.acquaint_schema(fixed_schema)

    enum_schema = {
        "type": "enum",
        "symbols": ["FOO"],
    }

    with pytest.raises(SchemaParseException):
        fastavro.acquaint_schema(enum_schema)

    # Should parse with name
    for schema in (record_schema, error_schema, fixed_schema, enum_schema):
        schema["name"] = "test_named_types_have_names"
        fastavro.acquaint_schema(schema)
