import random
from io import BytesIO
from fastavro import schemaless_writer, schemaless_reader
from fastavro.utils import generate_one, generate_many, anonymize_schema
from fastavro.schema import parse_schema
import pytest


def test_generate():
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {"name": "null", "type": "null"},
            {"name": "boolean", "type": "boolean"},
            {"name": "string", "type": "string"},
            {"name": "bytes", "type": "bytes"},
            {"name": "int", "type": "int"},
            {"name": "long", "type": "long"},
            {"name": "float", "type": "float"},
            {"name": "double", "type": "double"},
            {
                "name": "fixed",
                "type": {"type": "fixed", "name": "fixed_field", "size": 5},
            },
            {
                "name": "union",
                "type": [
                    "null",
                    "int",
                    {
                        "type": "record",
                        "name": "union_record",
                        "fields": [{"name": "union_record_field", "type": "string"}],
                    },
                ],
            },
            {
                "name": "enum",
                "type": {
                    "type": "enum",
                    "name": "enum_field",
                    "symbols": ["FOO", "BAR"],
                },
            },
            {"name": "array", "type": {"type": "array", "items": "string"}},
            {"name": "map", "type": {"type": "map", "values": "int"}},
            {
                "name": "record",
                "type": {
                    "type": "record",
                    "name": "subrecord",
                    "fields": [{"name": "sub_int", "type": "int"}],
                },
            },
            {"name": "named_type", "type": "subrecord"},
        ],
    }

    count = 10

    # Use list() to exhaust the generator)
    assert len(list(generate_many(schema, count))) == count


def test_anonymize():
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "doc": "this is a record",
        "fields": [
            {"name": "null", "type": "null"},
            {"name": "boolean", "type": "boolean"},
            {"name": "string", "type": "string", "default": "foo"},
            {"name": "bytes", "type": "bytes", "aliases": ["alias_field"]},
            {"name": "int", "type": "int", "doc": "doc"},
            {"name": "long", "type": "long"},
            {"name": "float", "type": "float"},
            {"name": "double", "type": "double"},
            {
                "name": "fixed",
                "type": {"type": "fixed", "name": "fixed_field", "size": 5},
            },
            {
                "name": "union",
                "type": [
                    "null",
                    "int",
                    {
                        "type": "record",
                        "name": "union_record",
                        "fields": [{"name": "union_record_field", "type": "string"}],
                    },
                ],
            },
            {"name": "array", "type": {"type": "array", "items": "string"}},
            {"name": "map", "type": {"type": "map", "values": "int"}},
            {
                "name": "record",
                "type": {
                    "type": "record",
                    "name": "subrecord",
                    "fields": [{"name": "sub_int", "type": "int"}],
                },
            },
            {"name": "named_type", "type": "subrecord"},
            {"name": "other_int", "type": {"type": "int"}},
        ],
    }

    anonymous_schema = anonymize_schema(schema)

    # Maintain random state so that other tests continue to be based off the
    # main starting seed
    seed_state = random.getstate()
    random.seed(1)
    record = generate_one(schema)

    random.seed(1)
    anonymous_record = generate_one(anonymous_schema)
    random.setstate(seed_state)

    bio1 = BytesIO()
    schemaless_writer(bio1, schema, record)

    bio2 = BytesIO()
    schemaless_writer(bio2, anonymous_schema, anonymous_record)

    assert bio1.getvalue() == bio2.getvalue()


def test_enum_symbols_get_anonymized():
    schema = {
        "type": "enum",
        "name": "enum_field",
        "symbols": ["FOO", "BAR"],
    }

    anonymous_schema = anonymize_schema(schema)

    assert anonymous_schema["symbols"] != schema["symbols"]


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2},
        {
            "name": "fixed_decimal",
            "type": "fixed",
            "size": 16,
            "logicalType": "decimal",
            "precision": 4,
            "scale": 2,
        },
        {"type": "string", "logicalType": "uuid"},
        {"type": "int", "logicalType": "date"},
        {"type": "int", "logicalType": "time-millis"},
        {"type": "long", "logicalType": "time-micros"},
        {"type": "long", "logicalType": "timestamp-millis"},
        {"type": "long", "logicalType": "timestamp-micros"},
        {"type": "long", "logicalType": "local-timestamp-millis"},
        {"type": "long", "logicalType": "local-timestamp-micros"},
    ],
)
def test_generate_logical_types(schema):
    anonymous_schema = parse_schema(schema)
    for record in generate_many(anonymous_schema, 100):
        try:
            bio = BytesIO()
            schemaless_writer(bio, schema, record)
            bio.seek(0)
            schemaless_reader(bio, schema)
        except Exception as e:
            raise RuntimeError(f"Failed for generated record: {record}") from e
