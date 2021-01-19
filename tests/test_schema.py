from os.path import join, abspath, dirname
import pytest
import fastavro
from fastavro.schema import (
    SchemaParseException,
    UnknownType,
    parse_schema,
    fullname,
    expand_schema,
)


def test_named_types_have_names():
    record_schema = {
        "type": "record",
        "fields": [
            {
                "name": "field",
                "type": "string",
            }
        ],
    }

    with pytest.raises(SchemaParseException):
        fastavro.parse_schema(record_schema)

    error_schema = {
        "type": "error",
        "fields": [
            {
                "name": "field",
                "type": "string",
            }
        ],
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
        "fields": [
            {
                "name": "field",
                "type": "string",
            }
        ],
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
        "fields": [
            {
                "name": "field",
                "type": "string",
                "aliases": ["test"],
            }
        ],
    }

    parsed_schema = parse_schema(schema)
    assert "aliases" in parsed_schema["fields"][0]


def test_aliases_is_a_list():
    """https://github.com/fastavro/fastavro/issues/206"""
    schema = {
        "type": "record",
        "name": "test_parse_schema",
        "fields": [
            {
                "name": "field",
                "type": "string",
                "aliases": "foobar",
            }
        ],
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
        SchemaParseException, match="decimal scale must be a postive integer"
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
        SchemaParseException, match="decimal scale must be a postive integer"
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
        match="decimal precision must be a postive integer",
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
        match="decimal precision must be a postive integer",
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
            {
                "name": "is_error",
                "type": "boolean",
                "default": False,
            },
            {
                "name": "outcome",
                "type": [
                    {
                        "type": "record",
                        "name": "SomeMessage",
                        "fields": [],
                    },
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
                "type": {
                    "type": "record",
                    "name": "ThisName",
                    "fields": [],
                },
            },
            {
                "name": "field2",
                "type": {
                    "type": "enum",
                    "name": "ThisName",
                    "symbols": ["FOO", "BAR"],
                },
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
                "type": {
                    "type": "record",
                    "name": "ThatName",
                    "fields": [],
                },
            },
            {
                "name": "field2",
                "type": {
                    "type": "fixed",
                    "name": "ThatName",
                    "size": 8,
                },
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
