# -*- coding: utf-8 -*-
from fastavro.validation import (
    ValidationError,
    ValidationErrorData,
    validate,
    validate_many,
)
from fastavro import parse_schema
import pytest
import numpy as np
from datetime import datetime

schema = {
    "fields": [
        {"name": "str_null", "type": ["null", "string"]},
        {"name": "str", "type": "string"},
        {"name": "integ_null", "type": ["null", "int"]},
        {"name": "integ", "type": "int"},
    ],
    "namespace": "namespace",
    "name": "missingerror",
    "type": "record",
}


# TODO: Add more test for all types and combinations


def validation_boolean(schema, *records):
    return validate_many(records, schema, raise_errors=False)


def validation_raise(schema, *records):
    return validate_many(records, schema, raise_errors=True)


def test_validate_string_in_int_raises():
    records = [
        {
            "str_null": "str",
            "str": "str",
            "integ_null": "str",
            "integ": 21,
        }
    ]

    with pytest.raises(ValidationError) as exc:
        validation_raise(schema, *records)

    for error in exc.value.errors:
        expected_type = error.schema
        assert expected_type in ["null", "int"]
        assert error.field == "namespace.missingerror.integ_null"


def test_validate_string_in_int_false():
    records = [
        {
            "str_null": "str",
            "str": "str",
            "integ_null": "str",
            "integ": 21,
        }
    ]

    assert validation_boolean(schema, *records) is False


def test_validate_true():
    records = [
        {
            "str_null": "str",
            "str": "str",
            "integ_null": 21,
            "integ": 21,
        },
        {
            "str_null": None,
            "str": "str",
            "integ_null": None,
            "integ": 21,
        },
    ]

    assert validation_boolean(schema, *records) is True
    validation_raise(schema, *records)


def test_validate_string_in_int_null_raises():
    records = [
        {
            "str_null": "str",
            "str": "str",
            "integ_null": 11,
            "integ": "str",
        }
    ]

    with pytest.raises(ValidationError) as exc:
        validation_raise(schema, *records)

    for error in exc.value.errors:
        expected_type = error.schema
        assert expected_type == "int"
        assert error.field == "namespace.missingerror.integ"


def test_validate_string_in_int_null_false():
    records = [
        {
            "str_null": "str",
            "str": "str",
            "integ_null": 11,
            "integ": "str",
        }
    ]

    assert validation_boolean(schema, *records) is False


def test_validate_int_in_string_null_raises():
    records = [
        {
            "str_null": 11,
            "str": "str",
            "integ_null": 21,
            "integ": 21,
        }
    ]

    with pytest.raises(ValidationError) as exc:
        validation_raise(schema, *records)

    for error in exc.value.errors:
        expected_type = error.schema
        assert expected_type in ["string", "null"]
        assert error.field == "namespace.missingerror.str_null"


def test_validate_int_in_string_null_false():
    records = [
        {
            "str_null": 11,
            "str": "str",
            "integ_null": 21,
            "integ": 21,
        }
    ]
    assert validation_boolean(schema, *records) is False


def test_validate_int_in_string_raises():
    records = [
        {
            "str_null": "str",
            "str": 11,
            "integ_null": 21,
            "integ": 21,
        }
    ]

    with pytest.raises(ValidationError) as exc:
        validation_raise(schema, *records)

    for error in exc.value.errors:
        expected_type = error.schema
        assert expected_type == "string"
        assert error.field == "namespace.missingerror.str"


def test_validate_int_in_string_false():
    records = [
        {
            "str_null": "str",
            "str": 11,
            "integ_null": 21,
            "integ": 21,
        }
    ]

    assert validation_boolean(schema, *records) is False


def test_validate_null_in_string_raises():
    records = [
        {
            "str_null": "str",
            "str": None,
            "integ_null": 21,
            "integ": 21,
        }
    ]

    with pytest.raises((ValidationError,)):
        validation_raise(schema, *records)


def test_validate_null_in_string_false():
    records = [
        {
            "str_null": "str",
            "str": None,
            "integ_null": 21,
            "integ": 21,
        }
    ]

    assert validation_boolean(schema, *records) is False


def test_validate_unicode_in_string_does_not_raise():
    """https://github.com/fastavro/fastavro/issues/269"""
    non_ascii = "日本語"

    records = [
        {
            "str_null": non_ascii,
            "str": "str",
            "integ_null": 21,
            "integ": 21,
        }
    ]

    validation_raise(schema, *records)

    records = [
        {
            "str_null": "str",
            "str": "str",
            "integ_null": 21,
            "integ": non_ascii,
        }
    ]

    with pytest.raises(ValidationError) as exc:
        validation_raise(schema, *records)

    for error in exc.value.errors:
        assert error.datum == non_ascii


def test_validate_error_raises():
    with pytest.raises(ValidationError):
        raise ValidationError()

    error = ValidationErrorData(10, "string", "test1")
    msg = "test1 is <10> of type <class 'int'> expected string"
    assert msg in str(error)


def test_validate_error_none_field():
    error = ValidationErrorData(10, "string", None)
    msg = " is <10> of type <class 'int'> expected string"
    assert msg in str(error)


def test_validator_numeric():
    for datum, schema in [
        (1, "int"),
        (1, "long"),
        (1.0, "float"),
        (1.0, "double"),
        (1, "float"),
        (1, "double"),
    ]:
        validate(datum, schema)

    for datum, schema in [
        (1.0, "int"),
        (1.0, "long"),
        ("1.0", "float"),
        ("1.0", "double"),
        ("1", "float"),
        ("1", "double"),
        (True, "int"),
        (True, "long"),
        (True, "float"),
        (True, "double"),
        (False, "int"),
        (False, "long"),
        (False, "float"),
        (False, "double"),
        (datetime(2020, 1, 1), "int"),
        (datetime(2020, 1, 1), "long"),
    ]:
        with pytest.raises(ValidationError):
            validate(datum, schema)
            pytest.fail(f"{datum} should not validate as {schema}")


def test_validator_logical():
    """https://github.com/fastavro/fastavro/issues/365"""
    for datum, schema in [
        (1, {"type": "long", "logicalType": "timestamp-micros"}),
    ]:
        validate(datum, schema)

    for datum, schema in [
        ("foo", {"type": "long", "logicalType": "timestamp-micros"}),
    ]:
        with pytest.raises(ValidationError):
            validate(datum, schema)
            pytest.fail(f"{datum} should not validate as {schema}")


def test_validate_array():
    my_schema = {
        "fields": [
            {
                "name": "array",
                "type": {
                    "type": "array",
                    "items": "string",
                },
            },
        ],
        "namespace": "namespace",
        "name": "test_validate_array",
        "type": "record",
    }

    datum = {"array": [1]}
    with pytest.raises(ValidationError) as exc:
        validate(datum, my_schema)

    for error in exc.value.errors:
        assert error.field == "namespace.test_validate_array.array"


def test_validate_map():
    my_schema = {
        "fields": [
            {
                "name": "map",
                "type": {
                    "type": "map",
                    "values": "string",
                },
            },
        ],
        "namespace": "namespace",
        "name": "test_validate_map",
        "type": "record",
    }

    datum = {"map": {"key": 1}}
    with pytest.raises(ValidationError) as exc:
        validate(datum, my_schema)

    for error in exc.value.errors:
        assert error.field == "namespace.test_validate_map.map"


def test_validator_numeric_numpy():
    np_ints = [
        np.int_,
        np.intc,
        np.intp,
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
    ]

    np_floats = [
        np.float_,
        np.float16,
        np.float32,
        np.float64,
    ]

    schema_ints = ["int", "long"]

    schema_floats = ["float", "double"]

    # all these should work
    for nptype, schema in zip(np_ints, schema_ints):
        validate(nptype(1), schema)

    for nptype, schema in zip(np_ints, schema_floats):
        validate(nptype(1), schema)

    for nptype, schema in zip(np_floats, schema_floats):
        validate(nptype(1), schema)

    # these shouldn't work
    for nptype, schema in zip(np_floats, schema_ints):
        with pytest.raises(ValidationError):
            validate(nptype(1), schema)


def test_validate_with_unparsed_schema():
    """https://github.com/fastavro/fastavro/issues/416"""
    schema = {
        "type": "record",
        "name": "my_schema_v1",
        "fields": [
            {
                "name": "field1",
                "type": {
                    "type": "record",
                    "name": "my_field_type",
                    "fields": [{"name": "sub_field", "type": "string"}],
                },
            },
            {"name": "field2", "type": "my_field_type"},
        ],
    }

    datum = {"field1": {"sub_field": "foo"}, "field2": {"sub_field": "bar"}}

    validate(datum, schema)


def test_validate_clobbering_SCHEMA_DEFS():
    """https://github.com/fastavro/fastavro/issues/416"""
    schema_1 = {
        "type": "record",
        "name": "my_schema_v1",
        "fields": [
            {
                "name": "field1",
                "type": {
                    "type": "record",
                    "name": "my_field_type",
                    "fields": [{"name": "sub_field", "type": "string"}],
                },
            },
            {"name": "field2", "type": "my_field_type"},
        ],
    }
    parsed_schema_1 = parse_schema(schema_1)

    datum_1 = {"field1": {"sub_field": "foo"}, "field2": {"sub_field": "bar"}}

    validate(datum_1, parsed_schema_1)

    schema_2 = {
        "type": "record",
        "name": "my_schema_v2",
        "fields": [
            {
                "name": "field1",
                "type": {
                    "type": "record",
                    "name": "my_field_type",
                    "fields": [{"name": "sub_field", "type": "int"}],
                },
            }
        ],
    }
    parse_schema(schema_2)

    validate(datum_1, parsed_schema_1)


def test_enum_named_type():
    """https://github.com/fastavro/fastavro/issues/450"""
    schema = {
        "type": "record",
        "name": "test_enum_named_type",
        "fields": [
            {
                "name": "test1",
                "type": {
                    "type": "enum",
                    "name": "my_enum",
                    "symbols": ["FOO", "BAR"],
                },
            },
            {
                "name": "test2",
                "type": "my_enum",
            },
        ],
    }

    record = {"test1": "FOO", "test2": "BAR"}
    parsed_schema = parse_schema(schema)
    validate(record, parsed_schema)


def test_fixed_named_type():
    """https://github.com/fastavro/fastavro/issues/450"""
    schema = {
        "type": "record",
        "name": "test_fixed_named_type",
        "fields": [
            {
                "name": "test1",
                "type": {
                    "type": "fixed",
                    "name": "my_fixed",
                    "size": 4,
                },
            },
            {
                "name": "test2",
                "type": "my_fixed",
            },
        ],
    }

    record = {"test1": b"1234", "test2": b"4321"}
    parsed_schema = parse_schema(schema)
    validate(record, parsed_schema)


def test_record_named_type():
    """https://github.com/fastavro/fastavro/issues/450"""
    schema = {
        "type": "record",
        "name": "test_record_named_type",
        "fields": [
            {
                "name": "test1",
                "type": {
                    "type": "record",
                    "name": "my_record",
                    "fields": [
                        {
                            "name": "field1",
                            "type": "string",
                        }
                    ],
                },
            },
            {
                "name": "test2",
                "type": "my_record",
            },
        ],
    }

    record = {"test1": {"field1": "foo"}, "test2": {"field1": "bar"}}
    parsed_schema = parse_schema(schema)
    validate(record, parsed_schema)


def test_record_name_with_named_type_in_union():
    schema = {
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

    records = [
        {
            "my_1st_union": ("foo", {"some_field": 1}),
            "my_2nd_union": ("bar", {"some_field": 2}),
        }
    ]

    parsed_schema = parse_schema(schema)
    validate_many(records, parsed_schema)


def test_validate_should_not_parse_schema_if_it_was_parsed_already():
    named_schemas = {}
    parse_schema(
        {"name": "B", "type": "record", "fields": [{"name": "bar", "type": "string"}]},
        named_schemas,
    )

    a_schema = parse_schema(
        {"name": "A", "type": "record", "fields": [{"name": "b", "type": "B"}]},
        named_schemas,
    )

    records = [{"b": {"bar": "bar"}}]

    validate_many(records, a_schema)
    validate(records[0], a_schema)


def test_validate_strict():
    """https://github.com/fastavro/fastavro/issues/423"""
    schema = {
        "type": "record",
        "name": "test_validate_strict",
        "fields": [
            {"name": "normal_field", "type": "string"},
            {"name": "normal_union_field", "type": ["int", "float"]},
            {"name": "nullable_union_field", "type": ["null", "string"]},
        ],
    }

    record = {"normal_field": "a", "normal_union_field": 1}
    parsed_schema = parse_schema(schema)
    with pytest.raises(ValidationError):
        validate(record, parsed_schema, strict=True)

    with pytest.raises(ValidationError):
        validate_many([record], parsed_schema, strict=True)
