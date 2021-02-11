import io
from fastavro.compile import ast_compile
from fastavro.write import schemaless_writer
import ast
import pytest
import decimal


class testcase:
    def __init__(self, label, schema, message=None, message_list=None):
        self.label = label
        self.schema = schema
        if message_list is not None:
            self.messages = message_list
        else:
            self.messages = []
        if message is not None:
            self.messages.append(message)

    def assert_reader(self):
        for i, m in enumerate(self.messages):
            message_encoded = io.BytesIO()
            schemaless_writer(message_encoded, self.schema, m)
            message_encoded.seek(0)

            sp = ast_compile.SchemaParser(self.schema)
            reader = sp.compile()
            have = reader(message_encoded)
            if len(self.messages) > 1:
                assert have == m, f"reader behavior mismatch for message idx={i}"
            else:
                assert have == m, f"reader behavior mismatch"


testcases = [
    testcase(
        label="primitive record",
        schema={
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
        },
        message={
            "string_field": "string_value",
            "int_field": 1,
            "long_field": 2,
            "float_field": 3.0,
            "double_field": -4.0,
            "boolean_field": True,
            "bytes_field": b"bytes_value",
            "null_field": None,
        },
    ),
    testcase(
        label="nested primitive record",
        schema={
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
                            {"type": "int", "name": "child_int_field"},
                        ],
                    },
                },
                {"type": "string", "name": "string_field"},
                {"type": "int", "name": "int_field"},
            ],
        },
        message={
            "string_field": "string_value",
            "int_field": 1,
            "child_field": {
                "child_string_field": "child_sting_value",
                "child_int_field": 2,
            },
        },
    ),
    testcase(
        label="name collisions in nested record",
        schema={
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
                            {"type": "int", "name": "int_field"},
                        ],
                    },
                },
                {"type": "string", "name": "string_field"},
                {"type": "int", "name": "int_field"},
            ],
        },
        message={
            "string_field": "string_value",
            "int_field": 1,
            "child_field": {
                "string_field": "child_sting_value",
                "int_field": 2,
            },
        },
    ),
    testcase(
        label="indirect primitive typename",
        schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {"type": {"type": "string"}, "name": "string_field"},
                {"type": {"type": "int"}, "name": "int_field"},
                {"type": {"type": "long"}, "name": "long_field"},
                {"type": {"type": "float"}, "name": "float_field"},
                {"type": {"type": "double"}, "name": "double_field"},
                {"type": {"type": "boolean"}, "name": "boolean_field"},
                {"type": {"type": "bytes"}, "name": "bytes_field"},
                {"type": {"type": "null"}, "name": "null_field"},
            ],
        },
        message={
            "string_field": "string_value",
            "int_field": 1,
            "long_field": 2,
            "float_field": 3.0,
            "double_field": -4.0,
            "boolean_field": True,
            "bytes_field": b"bytes_value",
            "null_field": None,
        },
    ),
    testcase(
        label="union_primitives",
        schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {
                    "name": "field",
                    "type": ["string", "long", "null", "boolean", "float"],
                },
            ],
        },
        message_list=[{"field": v} for v in ("string_val", 1, None, True, 0.5)],
    ),
    testcase(
        label="union of records",
        schema={
            "type": "record",
            "name": "Record",
            "fields": [
                {
                    "name": "field",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "subfield",
                            "fields": [{"type": "string", "name": "string_val"}],
                        },
                    ],
                },
            ],
        },
        message_list=[{"field": v} for v in [None, {"string_val": "abcd"}]],
    ),
    testcase(
        label="toplevel map",
        schema={"type": "map", "values": "int"},
        message={"key1": 1, "key2": 2, "key3": 3},
    ),
    testcase(
        label="toplevel array",
        schema={"type": "array", "items": "int"},
        message=[1, 2, 3],
    ),
    testcase(
        label="toplevel union",
        schema=["int", "string"],
        message="stringval",
    ),
    testcase(
        label="toplevel primitive",
        schema="int",
        message=42,
    ),
    testcase(
        label="toplevel enum",
        schema={"type": "enum", "name": "Foo", "symbols": ["A", "B", "C", "D"]},
        message="C"
    ),
    testcase(
        label="toplevel fixed",
        schema={"type": "fixed", "name": "md5", "size": 16},
        message=b"1234567812345678"
    ),
    testcase(
        label="logical decimal",
        schema={"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 4},
        message=decimal.Decimal("3.1415"),
    ),
    testcase(
        label="logical decimal without scale",
        schema={"type": "bytes", "logicalType": "decimal", "precision": 4},
        message=decimal.Decimal("1415"),
    )
]


@pytest.mark.parametrize("case", testcases, ids=[tc.label for tc in testcases])
def test_ast_compiler(case):
    case.assert_reader()


def test_ast_compiler_enum_with_default():

    writer_schema = {"type": "enum", "name": "Foo", "symbols": ["A", "B", "C", "D", "E"], "default": "A"}
    reader_schema = {"type": "enum", "name": "Foo", "symbols": ["A", "B", "C"], "default": "A"}
    message = "E"

    message_encoded = io.BytesIO()
    schemaless_writer(message_encoded, writer_schema, message)
    message_encoded.seek(0)

    sp = ast_compile.SchemaParser(reader_schema)
    reader = sp.compile()
    have = reader(message_encoded)

    assert have == "A"
