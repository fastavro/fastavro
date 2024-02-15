from io import BytesIO
import fastavro

import pytest


def roundtrip(schema, records, new_schema, **reader_options):
    new_file = BytesIO()
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)

    reader = fastavro.reader(new_file, new_schema, **reader_options)
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
    input_records = [{"key1": "value1", "key2": "value2", "key3": "value3"}]

    output_records = roundtrip(old_schema, input_records, new_schema)
    expected_records = [{"k1": "value1", "k2": "value2", "k3": "value3"}]
    assert output_records == expected_records


def test_alias_in_different_namespace():
    """Test alias to record in different namespace of the write schema"""

    old_schema = fastavro.parse_schema(
        {
            "type": "record",
            "namespace": "the.old.namespace",
            "name": "old_name",
            "fields": [{"name": "key1", "type": "string"}],
        }
    )

    new_schema = fastavro.parse_schema(
        {
            "type": "record",
            "namespace": "the.new.namespace",
            "name": "new_name",
            "aliases": ["the.old.namespace.old_name"],
            "fields": [{"name": "k1", "type": "string", "aliases": ["key1"]}],
        }
    )

    input_records = [{"key1": "foo"}]

    output_records = roundtrip(old_schema, input_records, new_schema)
    expected_records = [
        {
            "k1": "foo",
        }
    ]
    assert output_records == expected_records


def test_alias_in_union():
    """
    Test aliases in union to records in different namespace
    """

    old_schema = fastavro.parse_schema(
        {
            "type": "record",
            "namespace": "the.old.namespace",
            "name": "OldMainRecord",
            "fields": [
                {
                    "name": "main_union_old",
                    "type": [
                        {
                            "type": "record",
                            "name": "MessageA",
                            "fields": [{"name": "key1", "type": "string"}],
                        },
                        {
                            "type": "record",
                            "name": "MessageB",
                            "fields": [{"name": "key2", "type": "string"}],
                        },
                    ],
                }
            ],
        }
    )

    new_schema = fastavro.parse_schema(
        {
            "type": "record",
            "namespace": "the.new.namespace",
            "name": "NewMainRecord",
            "aliases": ["the.old.namespace.OldMainRecord"],
            "fields": [
                {
                    "name": "main_union_new",
                    "aliases": ["main_union_old"],
                    "type": [
                        {
                            "type": "record",
                            "name": "MsgA",
                            "aliases": ["the.old.namespace.MessageA"],
                            "fields": [
                                {"name": "k1", "type": "string", "aliases": ["key1"]}
                            ],
                        },
                        {
                            "type": "record",
                            "name": "MsgB",
                            "aliases": ["the.old.namespace.MessageB"],
                            "fields": [
                                {"name": "k2", "type": "string", "aliases": ["key2"]}
                            ],
                        },
                    ],
                }
            ],
        }
    )

    input_records = [
        {"main_union_old": ("the.old.namespace.MessageB", {"key2": "the value"})}
    ]

    output_records = roundtrip(old_schema, input_records, new_schema)
    expected_records = [
        {
            "main_union_new": {
                "k2": "the value",
            }
        }
    ]
    assert output_records == expected_records


@pytest.mark.parametrize(
    "reader_options", [{"return_record_name": True}, {"return_named_type": True}]
)
def test_re_encode_union_with_alias(reader_options):
    """
    Test the return_record_name and return_named_type flag for alias in a union.
    When renaming records (using aliases) in a new schema, the new name should be returned.
    """
    old_schema = {
        "type": "record",
        "name": "my_record",
        "fields": [
            {
                "name": "my_1st_union",
                "type": [
                    {
                        "name": "foo",
                        "type": "record",
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                    {
                        "name": "bar",
                        "type": "record",
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                ],
            },
            {"name": "my_2nd_union", "type": ["foo", "bar"]},
        ],
    }
    new_schema = {
        "type": "record",
        "name": "my_record",
        "fields": [
            {
                "name": "my_1st_union",
                "type": [
                    {
                        "name": "FOO_RECORD",
                        "type": "record",
                        "aliases": ["foo"],
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                    {
                        "name": "BAR_RECORD",
                        "type": "record",
                        "aliases": ["bar"],
                        "fields": [{"name": "some_field", "type": "int"}],
                    },
                ],
            },
            {"name": "my_2nd_union", "type": ["FOO_RECORD", "BAR_RECORD"]},
        ],
    }

    input_records = [
        {
            "my_1st_union": ("foo", {"some_field": 1}),
            "my_2nd_union": ("bar", {"some_field": 2}),
        }
    ]

    # perform schema resolution and return the record names
    output_records = roundtrip(old_schema, input_records, new_schema, **reader_options)

    # perform another roundtrip to ensure the returned record can be encoded with the new schema
    output_records = roundtrip(new_schema, output_records, new_schema, **reader_options)
    # new record names should be returned (instead of MessageB) since it is used in the reader schema
    expected_records = [
        {
            "my_1st_union": ("FOO_RECORD", {"some_field": 1}),
            "my_2nd_union": ("BAR_RECORD", {"some_field": 2}),
        }
    ]
    assert output_records == expected_records
