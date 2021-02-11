import io
from fastavro.compile import ast_compile
from fastavro.write import schemaless_writer
import ast


def assert_reader(schema, message):
    message_encoded = io.BytesIO()
    schemaless_writer(message_encoded, schema, message)
    message_encoded.seek(0)

    sp = ast_compile.SchemaParser(schema, "tst_reader")
    reader = sp.compile()
    have = reader(message_encoded)
    assert have == message



def test_compile_primitive_record():
    schema = {
        "type": "record",
        "name": "Record",
        "fields": [
            {"type": "string", "name": "string_field"},
            {"type": "int", "name": "int_field"},
            {"type": "long", "name": "long_field"},
            {"type": "float", "name": "float_field"},
            {"type": "double", "name": "double_field"},
            {"type": "boolean", "name": "boolean_field"},
            {"type": "bytes", "name": "bytes_field"},
            {"type": "null", "name": "null_field"},
        ],
    }

    message = {
        "string_field": "string_value",
        "int_field": 1,
        "long_field": 2,
        "float_field": 3.0,
        "double_field": -4.0,
        "boolean_field": True,
        "bytes_field": b"bytes_value",
        "null_field": None,
    }

    assert_reader(schema, message)


def test_compile_nested_primitive_record():
    schema = {
        "type": "record",
        "name": "Parent",
        "fields": [
            {
                "name": "child_field",
                "type": {
                    "type": "record",
                    "name": "Child",
                    "fields": [
                        {"type": "string", "name": "child_string_field"},
                        {"type": "int", "name": "child_int_field"}
                    ]
                 },
             },
            {"type": "string", "name": "string_field"},
            {"type": "int", "name": "int_field"},
        ],
    }

    message = {
        "string_field": "string_value",
        "int_field": 1,
        "child_field": {
            "child_string_field": "child_sting_value",
            "child_int_field": 2,
        }
    }

    assert_reader(schema, message)


def test_compile_nested_primitive_record_name_collisions():
    schema = {
        "type": "record",
        "name": "Parent",
        "fields": [
            {
                "name": "child_field",
                "type": {
                    "type": "record",
                    "name": "Child",
                    "fields": [
                        {"type": "string", "name": "string_field"},
                        {"type": "int", "name": "int_field"}
                    ]
                 },
             },
            {"type": "string", "name": "string_field"},
            {"type": "int", "name": "int_field"},
        ],
    }

    message = {
        "string_field": "string_value",
        "int_field": 1,
        "child_field": {
            "string_field": "child_sting_value",
            "int_field": 2,
        }
    }

    assert_reader(schema, message)


def test_compile_indirect_typename():
    schema = {
        "type": "record",
        "name": "Record",
        "fields": [
            {"type": {"type": "string"}, "name": "string_field"},
            {"type": {"type":  "int"}, "name": "int_field"},
            {"type": {"type": "long"}, "name": "long_field"},
            {"type": {"type": "float"}, "name": "float_field"},
            {"type": {"type": "double"}, "name": "double_field"},
            {"type": {"type": "boolean"}, "name": "boolean_field"},
            {"type": {"type": "bytes"}, "name": "bytes_field"},
            {"type": {"type": "null"}, "name": "null_field"},
        ],
    }

    message = {
        "string_field": "string_value",
        "int_field": 1,
        "long_field": 2,
        "float_field": 3.0,
        "double_field": -4.0,
        "boolean_field": True,
        "bytes_field": b"bytes_value",
        "null_field": None,
    }

    assert_reader(schema, message)
