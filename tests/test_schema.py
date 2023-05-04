from io import BytesIO
from os.path import join, abspath, dirname
import pytest
import fastavro
from fastavro.repository import AbstractSchemaRepository
from fastavro.schema import (
    SchemaParseException,
    UnknownType,
    parse_schema,
    fullname,
    expand_schema,
)


def test_named_types_have_names():
    record_schema = {"type": "record", "fields": [{"name": "field", "type": "string"}]}

    with pytest.raises(SchemaParseException):
        fastavro.parse_schema(record_schema)

    error_schema = {"type": "error", "fields": [{"name": "field", "type": "string"}]}

    with pytest.raises(SchemaParseException):
        fastavro.parse_schema(error_schema)

    fixed_schema = {"type": "fixed", "size": 1}

    with pytest.raises(SchemaParseException):
        fastavro.parse_schema(fixed_schema)

    enum_schema = {"type": "enum", "symbols": ["FOO"]}

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
        "fields": [{"name": "field", "type": "string"}],
    }

    parsed_schema = parse_schema(schema)
    assert "__fastavro_parsed" in parsed_schema

    parsed_schema_again = parse_schema(parsed_schema)
    assert parsed_schema_again == parsed_schema


def test_unknown_type():
    with pytest.raises(UnknownType):
        parse_schema({"type": "unknown"})


def test_aliases_are_preserved():
    schema = {
        "type": "record",
        "name": "test_parse_schema",
        "fields": [{"name": "field", "type": "string", "aliases": ["test"]}],
    }

    parsed_schema = parse_schema(schema)
    assert "aliases" in parsed_schema["fields"][0]


def test_aliases_is_a_list():
    """https://github.com/fastavro/fastavro/issues/206"""
    schema = {
        "type": "record",
        "name": "test_parse_schema",
        "fields": [{"name": "field", "type": "string", "aliases": "foobar"}],
    }

    with pytest.raises(SchemaParseException):
        parse_schema(schema)


def test_decimal_scale_is_an_int():
    """https://github.com/fastavro/fastavro/issues/262"""
    schema = {
        "type": "record",
        "name": "test_scale_is_an_int",
        "fields": [
            {
                "name": "field",
                "type": {
                    "logicalType": "decimal",
                    "precision": 5,
                    "scale": "2",
                    "type": "bytes",
                },
            }
        ],
    }

    with pytest.raises(
        SchemaParseException, match="decimal scale must be a positive integer"
    ):
        parse_schema(schema)


def test_decimal_scale_is_a_positive_int():
    """https://github.com/fastavro/fastavro/issues/457"""
    schema = {
        "type": "record",
        "name": "test_scale_is_an_int",
        "fields": [
            {
                "name": "field",
                "type": {
                    "logicalType": "decimal",
                    "precision": 5,
                    "scale": -2,
                    "type": "bytes",
                },
            }
        ],
    }

    with pytest.raises(
        SchemaParseException, match="decimal scale must be a positive integer"
    ):
        parse_schema(schema)


def test_decimal_precision_is_an_int():
    """https://github.com/fastavro/fastavro/issues/262"""
    schema = {
        "type": "record",
        "name": "test_scale_is_an_int",
        "fields": [
            {
                "name": "field",
                "type": {
                    "logicalType": "decimal",
                    "precision": "5",
                    "scale": 2,
                    "type": "bytes",
                },
            }
        ],
    }

    with pytest.raises(
        SchemaParseException,
        match="decimal precision must be a positive integer",
    ):
        parse_schema(schema)


def test_decimal_precision_is_a_positive_int():
    """https://github.com/fastavro/fastavro/issues/457"""
    schema = {
        "type": "record",
        "name": "test_scale_is_an_int",
        "fields": [
            {
                "name": "field",
                "type": {
                    "logicalType": "decimal",
                    "precision": -5,
                    "scale": 2,
                    "type": "bytes",
                },
            }
        ],
    }

    with pytest.raises(
        SchemaParseException,
        match="decimal precision must be a positive integer",
    ):
        parse_schema(schema)


def test_decimal_precision_is_greater_than_scale():
    """https://github.com/fastavro/fastavro/issues/457"""
    schema = {
        "type": "record",
        "name": "test_scale_is_an_int",
        "fields": [
            {
                "name": "field",
                "type": {
                    "logicalType": "decimal",
                    "precision": 5,
                    "scale": 10,
                    "type": "bytes",
                },
            }
        ],
    }

    with pytest.raises(
        SchemaParseException,
        match="decimal scale must be less than or equal to",
    ):
        parse_schema(schema)


def test_decimal_fixed_accommodates_precision():
    """https://github.com/fastavro/fastavro/issues/457"""
    schema = {
        "type": "record",
        "name": "test_scale_is_an_int",
        "fields": [
            {
                "name": "field",
                "type": {
                    "name": "fixed_decimal",
                    "logicalType": "decimal",
                    "precision": 10,
                    "scale": 2,
                    "type": "fixed",
                    "size": 2,
                },
            }
        ],
    }

    with pytest.raises(
        SchemaParseException,
        match=r"decimal precision of \d+ doesn't fit into array of length \d+",
    ):
        parse_schema(schema)


def test_named_type_cannot_be_redefined():
    schema = {
        "type": "record",
        "namespace": "test.avro.training",
        "name": "SomeMessage",
        "fields": [
            {"name": "is_error", "type": "boolean", "default": False},
            {
                "name": "outcome",
                "type": [
                    {"type": "record", "name": "SomeMessage", "fields": []},
                    {
                        "type": "record",
                        "name": "ErrorRecord",
                        "fields": [
                            {
                                "name": "errors",
                                "type": {"type": "map", "values": "string"},
                                "doc": "doc",
                            }
                        ],
                    },
                ],
            },
        ],
    }

    with pytest.raises(
        SchemaParseException,
        match="redefined named type: test.avro.training.SomeMessage",
    ):
        parse_schema(schema)

    schema = {
        "type": "record",
        "name": "SomeMessage",
        "fields": [
            {
                "name": "field1",
                "type": {"type": "record", "name": "ThisName", "fields": []},
            },
            {
                "name": "field2",
                "type": {"type": "enum", "name": "ThisName", "symbols": ["FOO", "BAR"]},
            },
        ],
    }

    with pytest.raises(SchemaParseException, match="redefined named type: ThisName"):
        parse_schema(schema)

    schema = {
        "type": "record",
        "name": "SomeMessage",
        "fields": [
            {
                "name": "field1",
                "type": {"type": "record", "name": "ThatName", "fields": []},
            },
            {
                "name": "field2",
                "type": {"type": "fixed", "name": "ThatName", "size": 8},
            },
        ],
    }

    with pytest.raises(SchemaParseException, match="redefined named type: ThatName"):
        parse_schema(schema)


def test_doc_left_in_parse_schema():
    schema = {
        "type": "record",
        "name": "test_doc_left_in_parse_schema",
        "doc": "blah",
        "fields": [{"name": "field1", "type": "string", "default": ""}],
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
        "fields": [{"name": "sub_field_1", "type": "string"}],
    }

    outer_schema = {
        "name": "MasterSchema",
        "namespace": "com.namespace.master",
        "type": "record",
        "fields": [
            {"name": "field_1", "type": "com.namespace.dependencies.Dependency"}
        ],
    }

    combined = {
        "name": "com.namespace.master.MasterSchema",
        "type": "record",
        "fields": [
            {
                "name": "field_1",
                "type": {
                    "name": "com.namespace.dependencies.Dependency",
                    "type": "record",
                    "fields": [{"name": "sub_field_1", "type": "string"}],
                },
            }
        ],
    }

    parsed = expand_schema([sub_schema, outer_schema])

    assert parsed[1] == combined


def test_schema_expansion_2():
    """https://github.com/fastavro/fastavro/issues/314"""
    original_schema = {
        "name": "MasterSchema",
        "namespace": "com.namespace.master",
        "type": "record",
        "fields": [
            {
                "name": "field_1",
                "type": {
                    "name": "Dependency",
                    "namespace": "com.namespace.dependencies",
                    "type": "record",
                    "fields": [{"name": "sub_field_1", "type": "string"}],
                },
            },
            {"name": "field_2", "type": "com.namespace.dependencies.Dependency"},
        ],
    }

    expanded_schema = {
        "name": "com.namespace.master.MasterSchema",
        "type": "record",
        "fields": [
            {
                "name": "field_1",
                "type": {
                    "name": "com.namespace.dependencies.Dependency",
                    "type": "record",
                    "fields": [{"name": "sub_field_1", "type": "string"}],
                },
            },
            {
                "name": "field_2",
                "type": {
                    "name": "com.namespace.dependencies.Dependency",
                    "type": "record",
                    "fields": [{"name": "sub_field_1", "type": "string"}],
                },
            },
        ],
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
        ],
    }

    parsed = expand_schema(sub_schema)
    assert sub_schema == parsed


def test_parse_schema_includes_hint_with_list():
    """https://github.com/fastavro/fastavro/issues/444"""
    schema = [
        {
            "type": "record",
            "name": "test_parse_schema_includes_hint_with_list_1",
            "doc": "blah",
            "fields": [{"name": "field1", "type": "string", "default": ""}],
        },
        {
            "type": "record",
            "name": "test_parse_schema_includes_hint_with_list_2",
            "doc": "blah",
            "fields": [{"name": "field2", "type": "string", "default": ""}],
        },
    ]
    parsed_schema = parse_schema(schema)
    for s in parsed_schema:
        assert "__fastavro_parsed" in s


def test_union_schemas_must_have_names_in_order():
    """https://github.com/fastavro/fastavro/issues/450"""
    schema1 = [
        {
            "name": "Location",
            "type": "record",
            "fields": [{"name": "city", "type": "long"}],
        },
        {
            "name": "Weather",
            "type": "record",
            "fields": [{"name": "of", "type": "Location"}],
        },
    ]
    # This should work because Location is defined first
    parse_schema(schema1)

    schema2 = [
        {
            "name": "Weather",
            "type": "record",
            "fields": [{"name": "of", "type": "Location"}],
        },
        {
            "name": "Location",
            "type": "record",
            "fields": [{"name": "city", "type": "long"}],
        },
    ]
    # This should not work because Location is defined after it is used
    with pytest.raises(UnknownType):
        parse_schema(schema2)


def test_using_named_schemas_to_handle_references():
    location = {
        "name": "Location",
        "type": "record",
        "fields": [{"name": "city", "type": "long"}],
    }
    weather = {
        "name": "Weather",
        "type": "record",
        "fields": [{"name": "of", "type": "Location"}],
    }

    named_schemas = {}
    parse_schema(location, named_schemas)
    parse_schema(weather, named_schemas)

    # This should not work because didn't supply the named schemas
    with pytest.raises(UnknownType):
        parse_schema(weather)


def test_load_schema_does_not_make_unions_of_unions():
    """https://github.com/fastavro/fastavro/issues/443"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path)
    assert isinstance(loaded_schema, dict)


def test_load_schema_does_not_make_unions_of_unions_2():
    """https://github.com/fastavro/fastavro/issues/443"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_2")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path)
    assert isinstance(loaded_schema, list)
    for schema in loaded_schema:
        assert not isinstance(schema, list)


def test_load_schema_output_is_correct():
    """https://github.com/fastavro/fastavro/issues/476"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = {
        "name": "A",
        "type": "record",
        "fields": [
            {
                "name": "b",
                "type": [
                    "null",
                    {
                        "name": "B",
                        "type": "record",
                        "fields": [
                            {
                                "name": "c",
                                "type": [
                                    "null",
                                    {
                                        "name": "C",
                                        "type": "record",
                                        "fields": [{"name": "foo", "type": "string"}],
                                    },
                                ],
                                "default": None,
                            }
                        ],
                    },
                ],
                "default": None,
            },
            {
                "name": "d",
                "type": [
                    "null",
                    {
                        "name": "D",
                        "type": "record",
                        "fields": [{"name": "bar", "type": "string"}],
                    },
                ],
                "default": None,
            },
        ],
    }
    assert loaded_schema == expected_schema


def test_load_schema_output_is_correct_3():
    """https://github.com/fastavro/fastavro/issues/476"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_3")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = {
        "name": "A",
        "type": "record",
        "fields": [
            {
                "name": "b",
                "type": [
                    "null",
                    {
                        "name": "B",
                        "type": "record",
                        "fields": [
                            {
                                "name": "c",
                                "type": [
                                    "null",
                                    {
                                        "name": "C",
                                        "type": "record",
                                        "fields": [{"name": "foo", "type": "string"}],
                                    },
                                ],
                                "default": None,
                            }
                        ],
                    },
                ],
                "default": None,
            },
            {"name": "c", "type": ["null", "C"], "default": None},
        ],
    }
    assert loaded_schema == expected_schema


def test_load_schema_output_is_correct_4():
    """https://github.com/fastavro/fastavro/issues/476"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_4")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = {
        "name": "A",
        "type": "record",
        "fields": [
            {
                "name": "b",
                "type": [
                    "null",
                    {
                        "name": "B",
                        "type": "record",
                        "fields": [{"name": "foo", "type": "string"}],
                    },
                ],
                "default": None,
            },
            {
                "name": "c",
                "type": [
                    "null",
                    {
                        "name": "C",
                        "type": "record",
                        "fields": [
                            {
                                "name": "b",
                                "type": ["null", "B"],
                                "default": None,
                            },
                        ],
                    },
                ],
                "default": None,
            },
        ],
    }
    assert loaded_schema == expected_schema


def test_load_schema_output_is_correct_5():
    """https://github.com/fastavro/fastavro/issues/476"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_5")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = {
        "name": "A",
        "type": "record",
        "fields": [
            {
                "name": "map_field",
                "type": {
                    "type": "map",
                    "values": {
                        "name": "B",
                        "type": "record",
                        "fields": [{"name": "foo", "type": "string"}],
                    },
                },
            },
            {"name": "b", "type": "B"},
        ],
    }
    assert loaded_schema == expected_schema


def test_load_schema_output_is_correct_6():
    """https://github.com/fastavro/fastavro/issues/476"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_6")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = {
        "name": "A",
        "type": "record",
        "fields": [
            {
                "name": "array_field",
                "type": {
                    "type": "array",
                    "items": {
                        "name": "B",
                        "type": "record",
                        "fields": [{"name": "foo", "type": "string"}],
                    },
                },
            },
            {"name": "b", "type": "B"},
        ],
    }
    assert loaded_schema == expected_schema


def test_load_schema_resolve_namespace():
    """https://github.com/fastavro/fastavro/issues/490"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_7")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = {
        "name": "namespace.A",
        "type": "record",
        "fields": [
            {
                "name": "b",
                "type": {
                    "name": "namespace.B",
                    "type": "record",
                    "fields": [{"name": "foo", "type": "string"}],
                },
            },
        ],
    }
    assert loaded_schema == expected_schema


def test_load_schema_resolve_namespace_enums():
    """https://github.com/fastavro/fastavro/issues/490"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_8")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = {
        "name": "namespace.A",
        "type": "record",
        "fields": [
            {
                "name": "b",
                "type": {
                    "name": "namespace.B",
                    "type": "enum",
                    "symbols": ["THIS", "THAT"],
                },
            },
            {
                "name": "c",
                "type": {
                    "name": "namespace.C",
                    "type": "enum",
                    "symbols": ["AND_THIS", "AND_THAT"],
                },
            },
        ],
    }
    assert loaded_schema == expected_schema


def test_load_schema_resolve_namespace_fixed():
    """https://github.com/fastavro/fastavro/issues/490"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_9")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = {
        "name": "namespace.A",
        "type": "record",
        "fields": [
            {
                "name": "b",
                "type": {
                    "name": "namespace.B",
                    "type": "fixed",
                    "size": 4,
                },
            },
            {
                "name": "c",
                "type": {
                    "name": "namespace.C",
                    "type": "fixed",
                    "size": 8,
                },
            },
        ],
    }
    assert loaded_schema == expected_schema


def test_load_schema_bug():
    """https://github.com/fastavro/fastavro/issues/494"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_10")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = {
        "name": "A",
        "type": "record",
        "fields": [
            {
                "name": "b",
                "type": [
                    "null",
                    {
                        "name": "B",
                        "type": "record",
                        "fields": [{"name": "foo", "type": "string"}],
                    },
                ],
                "default": None,
            },
            {
                "name": "c",
                "type": [
                    "null",
                    {
                        "name": "C",
                        "type": "record",
                        "fields": [
                            {
                                "name": "b",
                                "type": ["null", "B"],
                                "default": None,
                            },
                            {
                                "name": "d",
                                "type": [
                                    "null",
                                    {
                                        "name": "D",
                                        "type": "record",
                                        "fields": [{"name": "bar", "type": "string"}],
                                    },
                                ],
                                "default": None,
                            },
                        ],
                    },
                ],
                "default": None,
            },
        ],
    }
    assert loaded_schema == expected_schema


def test_load_schema_bug_2():
    """https://github.com/fastavro/fastavro/issues/494"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_11")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = {
        "name": "A",
        "type": "record",
        "fields": [
            {
                "name": "b",
                "type": {
                    "name": "B",
                    "type": "record",
                    "fields": [
                        {
                            "name": "e",
                            "type": {
                                "name": "E",
                                "type": "record",
                                "fields": [{"name": "baz", "type": "string"}],
                            },
                        },
                    ],
                },
            },
            {
                "name": "c",
                "type": {
                    "name": "C",
                    "type": "record",
                    "fields": [
                        {
                            "name": "d",
                            "type": {
                                "name": "D",
                                "type": "record",
                                "fields": [{"name": "bar", "type": "string"}],
                            },
                        },
                        {"name": "e", "type": "E"},
                    ],
                },
            },
        ],
    }
    assert loaded_schema == expected_schema


def test_load_schema_ordered():
    """https://github.com/fastavro/fastavro/issues/493"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_12")
    load_order = [
        join(load_schema_dir, "com", "namespace", "E.avsc"),
        join(load_schema_dir, "com", "namespace", "D.avsc"),
        join(load_schema_dir, "com", "C.avsc"),
        join(load_schema_dir, "com", "B.avsc"),
        join(load_schema_dir, "A.avsc"),
    ]

    loaded_schema = fastavro.schema.load_schema_ordered(load_order, _write_hint=False)

    expected_schema = {
        "name": "A",
        "type": "record",
        "fields": [
            {
                "name": "b",
                "type": {
                    "name": "com.B",
                    "type": "record",
                    "fields": [
                        {
                            "name": "e",
                            "type": {
                                "name": "com.namespace.E",
                                "type": "record",
                                "fields": [{"name": "baz", "type": "string"}],
                            },
                        },
                    ],
                },
            },
            {
                "name": "c",
                "type": {
                    "name": "com.C",
                    "type": "record",
                    "fields": [
                        {
                            "name": "d",
                            "type": {
                                "name": "com.namespace.D",
                                "type": "record",
                                "fields": [{"name": "bar", "type": "string"}],
                            },
                        },
                        {"name": "e", "type": "com.namespace.E"},
                    ],
                },
            },
        ],
    }
    assert loaded_schema == expected_schema


def test_load_schema_top_level_names():
    """https://github.com/fastavro/fastavro/issues/527"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_13")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = {
        "name": "B",
        "type": "record",
        "fields": [{"name": "foo", "type": "string"}],
    }
    assert loaded_schema == expected_schema


def test_load_schema_top_level_primitive():
    """https://github.com/fastavro/fastavro/issues/527"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_14")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = "string"
    assert loaded_schema == expected_schema


def test_load_schema_union_names():
    """https://github.com/fastavro/fastavro/issues/527"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_15")
    schema_path = join(load_schema_dir, "A.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path, _write_hint=False)

    expected_schema = [
        {
            "name": "B",
            "type": "record",
            "fields": [{"name": "foo", "type": "string"}],
        },
        {
            "name": "C",
            "type": "record",
            "fields": [{"name": "bar", "type": "string"}],
        },
    ]
    assert loaded_schema == expected_schema


def test_load_schema_accepts_custom_repository():
    class LocalSchemaRepository(AbstractSchemaRepository):
        def __init__(self, schemas):
            self.schemas = schemas

        def load(self, subject):
            return self.schemas.get(subject)

    repo = LocalSchemaRepository(
        {
            "A": {
                "name": "A",
                "type": "record",
                "fields": [{"name": "foo", "type": "B"}],
            },
            "B": {
                "name": "B",
                "type": "record",
                "fields": [{"name": "bar", "type": "string"}],
            },
        }
    )

    loaded_schema = fastavro.schema.load_schema("A", repo=repo, _write_hint=False)

    expected_schema = {
        "name": "A",
        "type": "record",
        "fields": [
            {
                "name": "foo",
                "type": {
                    "name": "B",
                    "type": "record",
                    "fields": [{"name": "bar", "type": "string"}],
                },
            },
        ],
    }
    assert loaded_schema == expected_schema


def test_schema_expansion_3():
    """https://github.com/fastavro/fastavro/issues/538"""
    references = {
        "com.namespace.dependencies.Dependency": {
            "name": "Dependency",
            "namespace": "com.namespace.dependencies",
            "type": "record",
            "fields": [{"name": "sub_field_1", "type": "string"}],
        }
    }

    original_schema = {
        "name": "MasterSchema",
        "namespace": "com.namespace.master",
        "type": "record",
        "fields": [
            {"name": "field_2", "type": "com.namespace.dependencies.Dependency"}
        ],
    }

    expected_expanded_schema_fields = [
        {
            "name": "field_2",
            "type": {
                "name": "Dependency",
                "namespace": "com.namespace.dependencies",
                "type": "record",
                "fields": [{"name": "sub_field_1", "type": "string"}],
            },
        }
    ]

    assert isinstance(original_schema, dict)

    try:
        parsed_schema = parse_schema(original_schema, named_schemas=references)
        assert expected_expanded_schema_fields == expand_schema(parsed_schema)["fields"]
    except UnknownType:
        pytest.fail(
            "expand_schema raised UnknownType even though referenced type is part of named_schemas"
        )


def test_explicit_null_namespace():
    """https://github.com/fastavro/fastavro/issues/537"""
    schema = {
        "type": "record",
        "name": "my_schema",
        "namespace": "",
        "fields": [{"name": "subfield", "type": "string"}],
    }
    parsed_schema = parse_schema(schema)
    assert parsed_schema["name"] == "my_schema"


def test_explicit_null_namespace_2():
    """https://github.com/fastavro/fastavro/issues/537"""
    schema = {
        "type": "record",
        "name": "my_schema",
        "namespace": None,
        "fields": [{"name": "subfield", "type": "string"}],
    }
    parsed_schema = parse_schema(schema)
    assert parsed_schema["name"] == "my_schema"


@pytest.mark.parametrize("symbol", [None, 0, "Ż", "0nope"])
def test_enum_symbols_validation__invalid(symbol):
    """https://github.com/fastavro/fastavro/issues/551"""
    invalid_schema = {
        "type": "record",
        "name": "my_schema",
        "fields": [
            {
                "name": "enum_field",
                "type": {
                    "name": "my_enum",
                    "type": "enum",
                    "symbols": [symbol],
                },
            }
        ],
    }

    with pytest.raises(SchemaParseException) as err:
        parse_schema(invalid_schema)

    assert (
        str(err.value)
        == "Every symbol must match the regular expression [A-Za-z_][A-Za-z0-9_]*"
    )


@pytest.mark.parametrize("symbol", ["OK", "_123", "None", "Enum"])
def test_enum_symbols_validation__correct(symbol):
    """https://github.com/fastavro/fastavro/issues/551"""
    invalid_schema = {
        "type": "record",
        "name": "my_schema",
        "fields": [
            {
                "name": "enum_field",
                "type": {
                    "name": "my_enum",
                    "type": "enum",
                    "symbols": [symbol],
                },
            }
        ],
    }

    try:
        parse_schema(invalid_schema)
    except SchemaParseException:
        pytest.fail(f"valid symbol {symbol} has been incorrectly marked as invalid.")


def test_enum_symbols_validation__uniqueness():
    """https://github.com/fastavro/fastavro/issues/551"""
    invalid_schema = {
        "type": "record",
        "name": "my_schema",
        "fields": [
            {
                "name": "enum_field",
                "type": {
                    "name": "my_enum",
                    "type": "enum",
                    "symbols": ["FOO", "BAR", "FOO"],
                },
            }
        ],
    }

    with pytest.raises(SchemaParseException) as err:
        parse_schema(invalid_schema)

    assert str(err.value) == "All symbols in an enum must be unique"


def test_enum_with_bad_default():
    """https://github.com/fastavro/fastavro/issues/563"""

    schema = {
        "type": "record",
        "name": "test_schema",
        "fields": [
            {
                "name": "test_enum",
                "type": {
                    "name": "test_enum_type",
                    "type": "enum",
                    "symbols": ["NONE"],
                    "default": "UNKNOWN",
                },
            }
        ],
    }

    with pytest.raises(
        SchemaParseException, match="Default value for enum must be in symbols list"
    ):
        fastavro.parse_schema(schema)


def test_foobar():
    """https://github.com/fastavro/fastavro/issues/608"""
    load_schema_dir = join(abspath(dirname(__file__)), "load_schema_test_16")
    schema_path = join(load_schema_dir, "namespace.match.avsc")
    loaded_schema = fastavro.schema.load_schema(schema_path)

    record = {
        "id": "123",
        "team1": {"name": "A"},
        "team2": {"name": "B"},
    }

    bio = BytesIO()
    fastavro.writer(bio, loaded_schema, [record])
    fastavro.writer(bio, loaded_schema, [record])

    bio.seek(0)

    records = list(fastavro.reader(bio))
    assert records == [record, record]


def test_default_matches_first_union_type():
    """https://github.com/fastavro/fastavro/issues/649"""

    schema = {
        "type": "record",
        "name": "test_default_matches_first_union_type",
        "fields": [
            {
                "name": "first_union",
                "type": ["string", "long"],
                "default": 10,
            }
        ],
    }

    with pytest.raises(
        SchemaParseException,
        match="Default value <10> must match first schema in union",
    ):
        fastavro.parse_schema(schema)


@pytest.mark.parametrize(
    "field_type,default_value",
    (
        ("string", 0),
        ({"type": "map", "values": "string"}, 0),
        ({"type": "enum", "name": "somename", "symbols": ["FOO"]}, 0),
        ({"type": "fixed", "name": "somename", "size": 4}, 0),
        ({"type": "record", "name": "somename", "fields": []}, 0),
        ({"type": "string"}, 0),
    ),
)
def test_default_matches_type(field_type, default_value):
    schema = {
        "type": "record",
        "name": "test_default_matches_type",
        "fields": [
            {
                "name": "field",
                "type": field_type,
                "default": default_value,
            }
        ],
    }

    with pytest.raises(
        SchemaParseException,
        match=f"Default value <{default_value}> must match schema type",
    ):
        fastavro.parse_schema(schema)


def test_correct_exception_text_for_default_value_test():
    """https://github.com/fastavro/fastavro/issues/649"""

    schema = {
        "type": "record",
        "name": "test_default_matches_first_union_type",
        "fields": [
            {
                "name": "first_union",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "internal",
                        "fields": [
                            {
                                "name": "internal_field",
                                "type": {"type": "array", "items": "string"},
                                "default": 10,
                            }
                        ],
                    },
                ],
                "default": None,
            }
        ],
    }

    with pytest.raises(
        SchemaParseException,
        match="Default value <10> must match schema type",
    ):
        fastavro.parse_schema(schema)


@pytest.mark.parametrize("field_type,default_value", (("float", 0), ("double", 0)))
def test_special_case_defaults(field_type, default_value):
    schema = {
        "type": "record",
        "name": "test_special_case_defaults",
        "fields": [
            {
                "name": "field",
                "type": field_type,
                "default": default_value,
            }
        ],
    }

    fastavro.parse_schema(schema)


def test_namespace_respected():
    """https://github.com/fastavro/fastavro/issues/690"""
    schema = {
        "type": "record",
        "name": "explicit_namespace.foo",
        "fields": [
            {
                "name": "field1",
                "type": {
                    "type": "record",
                    "name": "bar",
                    "fields": [{"name": "bar_field", "type": "int"}],
                },
            },
            {"name": "field2", "type": "explicit_namespace.bar"},
        ],
    }

    fastavro.parse_schema(schema)
