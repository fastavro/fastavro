import io
from fastavro.compile import ast_compile
import fastavro.write
import fastavro.read
import pytest
import decimal
import datetime
import uuid


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
            fastavro.write.schemaless_writer(message_encoded, self.schema, m)
            message_encoded.seek(0)

            sp = ast_compile.SchemaParser(self.schema)
            reader = sp.compile()
            have = reader(message_encoded)
            if len(self.messages) > 1:
                assert have == m, f"reader behavior mismatch for message idx={i}"
            else:
                assert have == m, "reader behavior mismatch"


testcases = [
    testcase(
        label="large record",
        schema={
            "type": "record",
            "name": "Record",
            "fields": [
                # Primitive types
                {"type": "string", "name": "string_field"},
                {"type": "int", "name": "int_field"},
                {"type": "long", "name": "long_field"},
                {"type": "float", "name": "float_field"},
                {"type": "double", "name": "double_field"},
                {"type": "boolean", "name": "boolean_field"},
                {"type": "bytes", "name": "bytes_field"},
                {"type": "null", "name": "null_field"},
                # Array types
                {
                    "name": "array_of_primitives",
                    "type": {
                        "type": "array",
                        "items": "int",
                    },
                },
                {
                    "name": "array_of_records",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "ArrayItem",
                            "fields": [{"name": "array_item_field", "type": "string"}],
                        },
                    },
                },
                {
                    "name": "array_of_records_with_arrays",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "ArrayItemWithSubarray",
                            "fields": [
                                {
                                    "name": "subarray",
                                    "type": {
                                        "type": "array",
                                        "items": "int",
                                    },
                                },
                            ],
                        },
                    },
                },
                {
                    "name": "array_of_maps",
                    "type": {
                        "type": "array",
                        "items": {"type": "map", "values": "boolean"},
                    },
                },
                # Maps
                {"name": "map_of_primitives", "type": {"type": "map", "values": "int"}},
                {
                    "name": "map_of_arrays",
                    "type": {
                        "type": "map",
                        "values": {"type": "array", "items": "int"},
                    },
                },
                {
                    "name": "map_of_records",
                    "type": {
                        "type": "map",
                        "values": {
                            "type": "record",
                            "name": "MapItem",
                            "fields": [{"name": "intval", "type": "int"}],
                        },
                    },
                },
                # Unions
                {"name": "union", "type": ["int", "boolean"]},
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
            "array_of_primitives": [5, 6, 7],
            "array_of_records": [
                {"array_item_field": "s1"},
                {"array_item_field": "s2"},
            ],
            "array_of_records_with_arrays": [
                {"subarray": [8, 9]},
                {"subarray": [10, 11]},
            ],
            "array_of_maps": [
                {"k1": True, "k2": False},
                {"k3": False},
                {"k4": True, "k5": True},
            ],
            "map_of_primitives": {"k6": 1, "k7": 2},
            "map_of_arrays": {"k8": [3, 4, 5], "k9": []},
            "map_of_records": {"k10": {"intval": 6}},
            "union": True,
        },
    ),
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
        label="map of primitives",
        schema={"type": "map", "values": "int"},
        message={"key1": 1, "key2": 2, "key3": 3},
    ),
    testcase(
        label="map of arrays",
        schema={"type": "map", "values": {"type": "array", "items": "int"}},
        message={"k8": [3, 4, 5], "k9": []},
    ),
    testcase(
        label="map of records",
        schema={
            "type": "map",
            "values": {
                "type": "record",
                "name": "item",
                "fields": [{"type": "int", "name": "intval"}],
            },
        },
        message={"k10": {"intval": 6}},
    ),
    testcase(
        label="array of primitives",
        schema={"type": "array", "items": "int"},
        message=[1, 2, 3],
    ),
    testcase(
        label="array of records",
        schema={
            "type": "array",
            "items": {
                "type": "record",
                "name": "ArrayItem",
                "fields": [{"name": "array_item_field", "type": "string"}],
            },
        },
        message=[
            {"array_item_field": "s1"},
            {"array_item_field": "s2"},
        ],
    ),
    testcase(
        label="array of records with arrays",
        schema={
            "type": "array",
            "items": {
                "type": "record",
                "name": "ArrayItemWithSubarray",
                "fields": [
                    {
                        "name": "subarray",
                        "type": {
                            "type": "array",
                            "items": "int",
                        },
                    },
                ],
            },
        },
        message=[
            {"subarray": [8, 9]},
            {"subarray": [10, 11]},
        ],
    ),
    testcase(
        label="array of maps",
        schema={
            "type": "array",
            "items": {"type": "map", "values": "boolean"},
        },
        message=[
            {"k1": True, "k2": False},
            {"k3": False},
            {"k4": True, "k5": True},
        ],
    ),
    testcase(
        label="union",
        schema=["int", "string"],
        message="stringval",
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
    testcase(label="optional", schema=["null", "int"], message=1),
    testcase(label="optional unset", schema=["null", "int"], message=None),
    testcase(label="backwards optional", schema=["int", "null"], message=1),
    testcase(label="backwards optional unset", schema=["int", "null"], message=None),
    testcase(
        label="toplevel primitive",
        schema="int",
        message=42,
    ),
    testcase(
        label="enum",
        schema={"type": "enum", "name": "Foo", "symbols": ["A", "B", "C", "D"]},
        message="C",
    ),
    testcase(
        label="fixed",
        schema={"type": "fixed", "name": "md5", "size": 16},
        message=b"1234567812345678",
    ),
    testcase(
        label="logical decimal",
        schema={"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 4},
        message=decimal.Decimal("3.1415"),
    ),
    testcase(
        label="logical fixed decimal",
        schema={
            "type": "fixed",
            "logicalType": "decimal",
            "precision": 5,
            "scale": 4,
            "size": 6,
            "name": "fixed_decimal",
        },
        message=decimal.Decimal("3.1415"),
    ),
    testcase(
        label="logical decimal without scale",
        schema={"type": "bytes", "logicalType": "decimal", "precision": 4},
        message=decimal.Decimal("1415"),
    ),
    testcase(
        label="logical decimal with unexpected type",
        schema={"type": "string", "logicalType": "decimal"},
        message="1.23",
    ),
    testcase(
        label="logical uuid",
        schema={"type": "string", "logicalType": "uuid"},
        message=uuid.UUID("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"),
    ),
    testcase(
        label="logical uuid with unexpected type",
        schema={"type": "int", "logicalType": "uuid"},
        message=1,
    ),
    testcase(
        label="logical date",
        schema={"type": "int", "logicalType": "date"},
        message=datetime.date(2021, 2, 11),
    ),
    testcase(
        label="logical date with unexpected type",
        schema={"type": "string", "logicalType": "date"},
        message="hello",
    ),
    testcase(
        label="logical time-millis",
        schema={"type": "int", "logicalType": "time-millis"},
        message=datetime.time(12, 3, 4, 5000),
    ),
    testcase(
        label="logical time-millis with unexpected type",
        schema={"type": "string", "logicalType": "time-millis"},
        message="hello",
    ),
    testcase(
        label="logical time-micros",
        schema={"type": "long", "logicalType": "time-micros"},
        message=datetime.time(12, 3, 4, 5),
    ),
    testcase(
        label="logical time-micros with unexpected type",
        schema={"type": "string", "logicalType": "time-micros"},
        message="hello",
    ),
    testcase(
        label="logical timestamp-millis",
        schema={"type": "long", "logicalType": "timestamp-millis"},
        message=datetime.datetime(
            2001, 2, 3, 4, 5, 6, 7000, tzinfo=datetime.timezone.utc
        ),
    ),
    testcase(
        label="logical timestamp-millis with unexpected type",
        schema={"type": "string", "logicalType": "timestamp-millis"},
        message="hello",
    ),
    testcase(
        label="logical timestamp-micros",
        schema={"type": "long", "logicalType": "timestamp-micros"},
        message=datetime.datetime(2001, 2, 3, 4, 5, 6, 7, tzinfo=datetime.timezone.utc),
    ),
    testcase(
        label="logical timestamp-micros with unexpected type",
        schema={"type": "string", "logicalType": "timestamp-micros"},
        message="hello",
    ),
    testcase(
        label="unknown logical type",
        schema={"type": "string", "logicalType": "made-up"},
        message="hello",
    ),
    testcase(
        label="recursive record",
        schema={
            "type": "record",
            "name": "LinkedListNode",
            "fields": [
                {"name": "value", "type": "string"},
                {"name": "next", "type": ["null", "LinkedListNode"]},
            ],
        },
        message={
            "value": "a",
            "next": {"value": "b", "next": {"value": "c", "next": None}},
        },
    ),
    testcase(
        label="embedded recursion record",
        schema={
            "type": "record",
            "name": "Wrapper",
            "fields": [
                {
                    "name": "list",
                    "type": {
                        "type": "record",
                        "name": "LinkedListNode",
                        "fields": [
                            {"name": "value", "type": "string"},
                            {"name": "next", "type": ["null", "LinkedListNode"]},
                        ],
                    },
                },
                {"name": "outer", "type": "int"},
            ],
        },
        message={
            "outer": 1,
            "list": {
                "value": "a",
                "next": {"value": "b", "next": {"value": "c", "next": None}},
            },
        },
    ),
    testcase(
        label="nested recursion",
        schema={
            "type": "record",
            "name": "Outer",
            "fields": [
                {
                    "name": "outer2middle",
                    "type": {
                        "name": "Middle",
                        "type": "record",
                        "fields": [
                            {
                                "name": "middle2inner",
                                "type": {
                                    "name": "Inner",
                                    "type": "record",
                                    "fields": [
                                        {
                                            "name": "inner2outer",
                                            "type": ["null", "Outer"],
                                        },
                                        {
                                            "name": "inner2middle",
                                            "type": ["null", "Middle"],
                                        },
                                    ],
                                },
                            },
                            {
                                "name": "middle2outer",
                                "type": ["null", "Outer"],
                            },
                        ],
                    },
                },
                {"name": "outer2inner", "type": ["null", "Inner"]},
            ],
        },
        message={
            "outer2middle": {
                "middle2inner": {
                    "inner2outer": {
                        "outer2middle": {
                            "middle2inner": {
                                "inner2outer": None,
                                "inner2middle": None,
                            },
                            "middle2outer": None,
                        },
                        "outer2inner": None,
                    },
                    "inner2middle": {
                        "middle2inner": {
                            "inner2outer": None,
                            "inner2middle": None,
                        },
                        "middle2outer": None,
                    },
                },
                "middle2outer": None,
            },
            "outer2inner": None,
        },
    ),
    testcase(
        label="illegal name",
        schema={
            "type": "record",
            "name": "$illegal$outer$name",
            "fields": [
                {
                    "name": "inner_array",
                    "type": {
                        "type": "array",
                        "items": {
                            "name": "$illegal$name",
                            "type": "fixed",
                            "size": 8,
                        },
                    },
                }
            ],
        },
        message={
            "inner_array": [b"12345678"],
        },
    ),
    testcase(
        label="blank name",
        schema={
            "type": "record",
            "name": "Outer",
            "fields": [
                {
                    "name": "inner_array",
                    "type": {
                        "type": "array",
                        "items": {
                            "name": "$",
                            "type": "fixed",
                            "size": 8,
                        },
                    },
                }
            ],
        },
        message={
            "inner_array": [b"12345678"],
        },
    ),
]


@pytest.mark.parametrize("case", testcases, ids=[tc.label for tc in testcases])
def test_ast_compiler(case):
    case.assert_reader()


def test_ast_compiler_enum_with_default():

    writer_schema = {
        "type": "enum",
        "name": "Foo",
        "symbols": ["A", "B", "C", "D", "E"],
        "default": "A",
    }
    reader_schema = {
        "type": "enum",
        "name": "Foo",
        "symbols": ["A", "B", "C"],
        "default": "A",
    }
    message = "E"

    message_encoded = io.BytesIO()
    fastavro.write.schemaless_writer(message_encoded, writer_schema, message)
    message_encoded.seek(0)

    sp = ast_compile.SchemaParser(reader_schema)
    reader = sp.compile()
    have = reader(message_encoded)

    assert have == "A"


def test_ast_compiler_read_file():
    schema = {
        "type": "record",
        "name": "Record",
        "fields": [
            # Primitive types
            {"type": "int", "name": "int_field"},
        ],
    }
    records = [{"int_field": x} for x in range(10000)]
    new_file = io.BytesIO()
    fastavro.write.writer(new_file, schema, records)
    new_file.seek(0)

    official_reader = fastavro.read.reader(new_file)
    want_records = list(official_reader)

    new_file.seek(0)
    have_records = list(ast_compile.read_file(new_file))
    assert have_records == want_records
