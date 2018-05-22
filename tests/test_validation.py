from fastavro.validation import (
    ValidationError,
    ValidationErrorData,
    validate,
    validate_many
)
import pytest
import numpy as np
import sys

# In PY2 when you do type(int) you get <type 'type'> but in PY3 you get
# <class 'type'>
if sys.version_info >= (3, 0):
    type_type = 'class'
else:
    type_type = 'type'

schema = {
    "fields": [
        {
            "name": "str_null",
            "type": ["null", "string"]
        },
        {
            "name": "str",
            "type": "string"
        },
        {
            "name": "integ_null",
            "type": ["null", "int"]
        },
        {
            "name": "integ",
            "type": "int"
        }
    ],
    "namespace": "namespace",
    "name": "missingerror",
    "type": "record"
}


# TODO: Add more test for all types and combinations


def validation_boolean(schema, *records):
    return validate_many(records, schema, raise_errors=False)


def validation_raise(schema, *records):
    return validate_many(records, schema, raise_errors=True)


def test_validate_string_in_int_raises():
    records = [{
        'str_null': 'str',
        'str': 'str',
        'integ_null': 'str',
        'integ': 21,
    }]

    with pytest.raises(ValidationError) as exc:
        validation_raise(schema, *records)

    for error in exc.value.errors:
        expected_type = error.schema
        assert expected_type in ['null', 'int']
        assert error.field == 'namespace.missingerror.integ_null'


def test_validate_string_in_int_false():
    records = [{
        'str_null': 'str',
        'str': 'str',
        'integ_null': 'str',
        'integ': 21,
    }]

    assert validation_boolean(schema, *records) is False


def test_validate_true():
    records = [
        {'str_null': 'str', 'str': 'str', 'integ_null': 21, 'integ': 21, },
        {'str_null': None, 'str': 'str', 'integ_null': None, 'integ': 21, },
    ]

    assert validation_boolean(schema, *records) is True


def test_validate_string_in_int_null_raises():
    records = [{
        'str_null': 'str',
        'str': 'str',
        'integ_null': 11,
        'integ': 'str',
    }]

    with pytest.raises(ValidationError) as exc:
        validation_raise(schema, *records)

    for error in exc.value.errors:
        expected_type = error.schema
        assert expected_type == 'int'
        assert error.field == 'namespace.missingerror.integ'


def test_validate_string_in_int_null_false():
    records = [{
        'str_null': 'str',
        'str': 'str',
        'integ_null': 11,
        'integ': 'str',
    }]

    assert validation_boolean(schema, *records) is False


def test_validate_int_in_string_null_raises():
    records = [{
        'str_null': 11,
        'str': 'str',
        'integ_null': 21,
        'integ': 21,
    }]

    with pytest.raises(ValidationError) as exc:
        validation_raise(schema, *records)

    for error in exc.value.errors:
        expected_type = error.schema
        assert expected_type in ['string', 'null']
        assert error.field == 'namespace.missingerror.str_null'


def test_validate_int_in_string_null_false():
    records = [{
        'str_null': 11,
        'str': 'str',
        'integ_null': 21,
        'integ': 21,
    }]
    assert validation_boolean(schema, *records) is False


def test_validate_int_in_string_raises():
    records = [{
        'str_null': 'str',
        'str': 11,
        'integ_null': 21,
        'integ': 21,
    }]

    with pytest.raises(ValidationError) as exc:
        validation_raise(schema, *records)

    for error in exc.value.errors:
        expected_type = error.schema
        assert expected_type == 'string'
        assert error.field == 'namespace.missingerror.str'


def test_validate_int_in_string_false():
    records = [{
        'str_null': 'str',
        'str': 11,
        'integ_null': 21,
        'integ': 21,
    }]

    assert validation_boolean(schema, *records) is False


def test_validate_null_in_string_raises():
    records = [{
        'str_null': 'str',
        'str': None,
        'integ_null': 21,
        'integ': 21,
    }]

    with pytest.raises((ValidationError,)):
        validation_raise(schema, *records)


def test_validate_null_in_string_false():
    records = [{
        'str_null': 'str',
        'str': None,
        'integ_null': 21,
        'integ': 21,
    }]

    assert validation_boolean(schema, *records) is False


def test_validate_error_raises():
    with pytest.raises(ValidationError):
        raise ValidationError()

    error = ValidationErrorData(10, "string", "test1")
    msg = "test1 is <10> of type <{} 'int'> expected string".format(type_type)
    assert msg in str(error)


def test_validator_numeric():
    for datum, schema in [
        (1, 'int'),
        (1, 'long'),
        (1.0, 'float'),
        (1.0, 'double'),
        (1, 'float'),
        (1, 'double'),
    ]:
        validate(datum, schema)

    for datum, schema in [
        (1.0, 'int'),
        (1.0, 'long'),
        ("1.0", 'float'),
        ("1.0", 'double'),
        ("1", 'float'),
        ("1", 'double'),
    ]:
        with pytest.raises(ValidationError):
            validate(datum, schema)
    # and plenty more to add I suppose


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

    schema_ints = ['int', 'long']

    schema_floats = ['float', 'double']

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
