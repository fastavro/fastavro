import pytest
from fastavro.schema import to_parsing_canonical_form
from fastavro._schema_common import PRIMITIVES


@pytest.mark.parametrize(
    "original_schema,canonical_form",
    (
        [(primitive, f'"{primitive}"') for primitive in PRIMITIVES]
        + [({"type": primitive}, f'"{primitive}"') for primitive in PRIMITIVES]
    ),
)
def test_primitive_conversion(original_schema, canonical_form):
    assert to_parsing_canonical_form(original_schema) == canonical_form


def test_fullname_conversion():
    schema = {
        "namespace": "namespace",
        "name": "test_fullname_conversion",
        "type": "record",
        "fields": [],
    }

    assert (
        to_parsing_canonical_form(schema)
        == '{"name":"namespace.test_fullname_conversion","type":"record","fields":[]}'
    )


def test_fullname_conversion_empty_namespace():
    schema = {
        "namespace": "",
        "name": "test_fullname_conversion_empty_namespace",
        "type": "record",
        "fields": [],
    }

    assert (
        to_parsing_canonical_form(schema)
        == '{"name":"test_fullname_conversion_empty_namespace","type":"record","fields":[]}'
    )


def test_fullname_conversion_no_namespace():
    schema = {
        "name": "test_fullname_conversion_no_namespace",
        "type": "record",
        "fields": [],
    }

    assert (
        to_parsing_canonical_form(schema)
        == '{"name":"test_fullname_conversion_no_namespace","type":"record","fields":[]}'
    )


def test_remove_doc():
    schema = {"name": "test_remove_doc", "type": "record", "fields": [], "doc": "doc"}

    assert (
        to_parsing_canonical_form(schema)
        == '{"name":"test_remove_doc","type":"record","fields":[]}'
    )


def test_remove_aliases():
    schema = {
        "name": "test_remove_aliases",
        "type": "record",
        "fields": [],
        "aliases": "alias",
    }

    assert (
        to_parsing_canonical_form(schema)
        == '{"name":"test_remove_aliases","type":"record","fields":[]}'
    )


def test_record_field_order():
    schema = {
        "fields": [],
        "name": "test_record_field_order",
        "type": "record",
    }

    assert (
        to_parsing_canonical_form(schema)
        == '{"name":"test_record_field_order","type":"record","fields":[]}'
    )


def test_enum_field_order():
    schema = {
        "symbols": ["A", "B"],
        "name": "test_enum_field_order",
        "type": "enum",
    }

    assert (
        to_parsing_canonical_form(schema)
        == '{"name":"test_enum_field_order","type":"enum","symbols":["A","B"]}'
    )


def test_array_field_order():
    schema = {"items": "int", "type": "array"}

    assert to_parsing_canonical_form(schema) == '{"type":"array","items":"int"}'


def test_map_field_order():
    schema = {"values": "int", "type": "map"}

    assert to_parsing_canonical_form(schema) == '{"type":"map","values":"int"}'


def test_fixed_field_order():
    schema = {
        "size": 4,
        "name": "test_fixed_field_order",
        "type": "fixed",
    }

    assert (
        to_parsing_canonical_form(schema)
        == '{"name":"test_fixed_field_order","type":"fixed","size":4}'
    )


@pytest.mark.parametrize(
    "original_schema,canonical_form",
    [
        (
            {
                "type": "array",
                "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
            },
            '{"type":"array","items":{"name":"Test","type":"enum","symbols":["A","B"]}}',
        ),
        (
            {
                "type": "map",
                "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
            },
            '{"type":"map","values":{"name":"Test","type":"enum","symbols":["A","B"]}}',
        ),
        (
            ["string", "null", "long"],
            '["string","null","long"]',
        ),
        (
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}],
            },
            '{"name":"Test","type":"record","fields":[{"name":"f","type":"long"}]}',
        ),
        (
            {
                "type": "error",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}],
            },
            '{"name":"Test","type":"record","fields":[{"name":"f","type":"long"}]}',
        ),
        (
            {
                "type": "record",
                "name": "Node",
                "fields": [
                    {"name": "label", "type": "string"},
                    {"name": "children", "type": {"type": "array", "items": "Node"}},
                ],
            },
            (
                '{"name":"Node","type":"record","fields":[{"name":"label","type'
                + '":"string"},{"name":"children","type":{"type":"array","items'
                + '":"Node"}}]}'
            ),
        ),
        (
            {
                "type": "record",
                "name": "Lisp",
                "fields": [
                    {
                        "name": "value",
                        "type": [
                            "null",
                            "string",
                            {
                                "type": "record",
                                "name": "Cons",
                                "fields": [
                                    {"name": "car", "type": "Lisp"},
                                    {"name": "cdr", "type": "Lisp"},
                                ],
                            },
                        ],
                    },
                ],
            },
            (
                '{"name":"Lisp","type":"record","fields":[{"name":"value","type'
                + '":["null","string",{"name":"Cons","type":"record","fields":['
                + '{"name":"car","type":"Lisp"},{"name":"cdr","type":"Lisp"}]}]'
                + "}]}"
            ),
        ),
        (
            {
                "type": "record",
                "name": "HandshakeRequest",
                "namespace": "org.apache.avro.ipc",
                "fields": [
                    {
                        "name": "clientHash",
                        "type": {"type": "fixed", "name": "MD5", "size": 16},
                    },
                    {"name": "clientProtocol", "type": ["null", "string"]},
                    {"name": "serverHash", "type": "MD5"},
                    {
                        "name": "meta",
                        "type": ["null", {"type": "map", "values": "bytes"}],
                    },
                ],
            },
            (
                '{"name":"org.apache.avro.ipc.HandshakeRequest","type":"record"'
                + ',"fields":[{"name":"clientHash","type":{"name":"org.apache.a'
                + 'vro.ipc.MD5","type":"fixed","size":16}},{"name":"clientProto'
                + 'col","type":["null","string"]},{"name":"serverHash","type":"'
                + 'org.apache.avro.ipc.MD5"},{"name":"meta","type":["null",{"ty'
                + 'pe":"map","values":"bytes"}]}]}'
            ),
        ),
        (
            {
                "type": "record",
                "name": "HandshakeResponse",
                "namespace": "org.apache.avro.ipc",
                "fields": [
                    {
                        "name": "match",
                        "type": {
                            "type": "enum",
                            "name": "HandshakeMatch",
                            "symbols": ["BOTH", "CLIENT", "NONE"],
                        },
                    },
                    {"name": "serverProtocol", "type": ["null", "string"]},
                    {
                        "name": "serverHash",
                        "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}],
                    },
                    {
                        "name": "meta",
                        "type": ["null", {"type": "map", "values": "bytes"}],
                    },
                ],
            },
            (
                '{"name":"org.apache.avro.ipc.HandshakeResponse","type":"record'
                + '","fields":[{"name":"match","type":{"name":"org.apache.avro.'
                + 'ipc.HandshakeMatch","type":"enum","symbols":["BOTH","CLIENT"'
                + ',"NONE"]}},{"name":"serverProtocol","type":["null","string"]'
                + '},{"name":"serverHash","type":["null",{"name":"org.apache.av'
                + 'ro.ipc.MD5","type":"fixed","size":16}]},{"name":"meta","type'
                + '":["null",{"type":"map","values":"bytes"}]}]}'
            ),
        ),
        (
            {
                "type": "record",
                "name": "Interop",
                "namespace": "org.apache.avro",
                "fields": [
                    {"name": "intField", "type": "int"},
                    {"name": "longField", "type": "long"},
                    {"name": "stringField", "type": "string"},
                    {"name": "boolField", "type": "boolean"},
                    {"name": "floatField", "type": "float"},
                    {"name": "doubleField", "type": "double"},
                    {"name": "bytesField", "type": "bytes"},
                    {"name": "nullField", "type": "null"},
                    {
                        "name": "arrayField",
                        "type": {"type": "array", "items": "double"},
                    },
                    {
                        "name": "mapField",
                        "type": {
                            "type": "map",
                            "values": {
                                "name": "Foo",
                                "type": "record",
                                "fields": [{"name": "label", "type": "string"}],
                            },
                        },
                    },
                    {
                        "name": "unionField",
                        "type": [
                            "boolean",
                            "double",
                            {"type": "array", "items": "bytes"},
                        ],
                    },
                    {
                        "name": "enumField",
                        "type": {
                            "type": "enum",
                            "name": "Kind",
                            "symbols": ["A", "B", "C"],
                        },
                    },
                    {
                        "name": "fixedField",
                        "type": {"type": "fixed", "name": "MD5", "size": 16},
                    },
                    {
                        "name": "recordField",
                        "type": {
                            "type": "record",
                            "name": "Node",
                            "fields": [
                                {"name": "label", "type": "string"},
                                {
                                    "name": "children",
                                    "type": {"type": "array", "items": "Node"},
                                },
                            ],
                        },
                    },
                ],
            },
            (
                '{"name":"org.apache.avro.Interop","type":"record","fields":[{"'
                + 'name":"intField","type":"int"},{"name":"longField","type":"l'
                + 'ong"},{"name":"stringField","type":"string"},{"name":"boolFi'
                + 'eld","type":"boolean"},{"name":"floatField","type":"float"},'
                + '{"name":"doubleField","type":"double"},{"name":"bytesField",'
                + '"type":"bytes"},{"name":"nullField","type":"null"},{"name":"'
                + 'arrayField","type":{"type":"array","items":"double"}},{"name'
                + '":"mapField","type":{"type":"map","values":{"name":"org.apac'
                + 'he.avro.Foo","type":"record","fields":[{"name":"label","type'
                + '":"string"}]}}},{"name":"unionField","type":["boolean","doub'
                + 'le",{"type":"array","items":"bytes"}]},{"name":"enumField","'
                + 'type":{"name":"org.apache.avro.Kind","type":"enum","symbols"'
                + ':["A","B","C"]}},{"name":"fixedField","type":{"name":"org.ap'
                + 'ache.avro.MD5","type":"fixed","size":16}},{"name":"recordFie'
                + 'ld","type":{"name":"org.apache.avro.Node","type":"record","f'
                + 'ields":[{"name":"label","type":"string"},{"name":"children",'
                + '"type":{"type":"array","items":"org.apache.avro.Node"}}]}}]}'
            ),
        ),
        (
            {
                "type": "record",
                "name": "ipAddr",
                "fields": [
                    {
                        "name": "addr",
                        "type": [
                            {"name": "IPv6", "type": "fixed", "size": 16},
                            {"name": "IPv4", "type": "fixed", "size": 4},
                        ],
                    }
                ],
            },
            (
                '{"name":"ipAddr","type":"record","fields":[{"name":"addr","typ'
                + 'e":[{"name":"IPv6","type":"fixed","size":16},{"name":"IPv4",'
                + '"type":"fixed","size":4}]}]}'
            ),
        ),
        (
            {
                "type": "record",
                "name": "TestDoc",
                "doc": "Doc string",
                "fields": [{"name": "name", "type": "string", "doc": "Doc String"}],
            },
            (
                '{"name":"TestDoc","type":"record","fields":[{"name":"name","ty'
                + 'pe":"string"}]}'
            ),
        ),
        (
            {
                "type": "enum",
                "name": "Test",
                "symbols": ["A", "B"],
                "doc": "Doc String",
            },
            '{"name":"Test","type":"enum","symbols":["A","B"]}',
        ),
    ],
)
def test_random_cases(original_schema, canonical_form):
    # All of these random test cases came from the test cases here:
    # https://github.com/apache/avro/blob/0552c674637dd15b8751ed5181387cdbd81480d5/lang/py3/avro/tests/test_normalization.py
    assert to_parsing_canonical_form(original_schema) == canonical_form
