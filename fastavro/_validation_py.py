import array
import numbers
from collections.abc import Mapping, Sequence

from fastavro.const import INT_MAX_VALUE, INT_MIN_VALUE, LONG_MAX_VALUE, LONG_MIN_VALUE
from ._validate_common import ValidationError, ValidationErrorData
from .schema import extract_record_type, extract_logical_type, schema_name, parse_schema
from .logical_writers import LOGICAL_WRITERS
from ._schema_common import UnknownType


def validate_null(datum, **kwargs):
    """
    Checks that the data value is None.

    Parameters
    ----------
    datum: Any
        Data being validated
    kwargs: Any
        Unused kwargs
    """
    return datum is None


def validate_boolean(datum, **kwargs):
    """
    Check that the data value is bool instance

    Parameters
    ----------
    datum: Any
        Data being validated
    kwargs: Any
        Unused kwargs
    """
    return isinstance(datum, bool)


def validate_string(datum, **kwargs):
    """
    Check that the data value is string

    Parameters
    ----------
    datum: Any
        Data being validated
    kwargs: Any
        Unused kwargs
    """
    return isinstance(datum, str)


def validate_bytes(datum, **kwargs):
    """
    Check that the data value is python bytes type

    Parameters
    ----------
    datum: Any
        Data being validated
    kwargs: Any
        Unused kwargs
    """
    return isinstance(datum, (bytes, bytearray))


def validate_int(datum, **kwargs):
    """
    Check that the data value is a non floating
    point number with size less that Int32.

    Int32 = -2147483648<=datum<=2147483647

    conditional python types: int, numbers.Integral

    Parameters
    ----------
    datum: Any
        Data being validated
    kwargs: Any
        Unused kwargs
    """
    return (
        isinstance(datum, (int, numbers.Integral))
        and INT_MIN_VALUE <= datum <= INT_MAX_VALUE
        and not isinstance(datum, bool)
    )


def validate_long(datum, **kwargs):
    """
    Check that the data value is a non floating
    point number with size less that long64.

    Int64 = -9223372036854775808 <= datum <= 9223372036854775807

    conditional python types: int, numbers.Integral

    :Parameters
    ----------
    datum: Any
        Data being validated
    kwargs: Any
        Unused kwargs
    """
    return (
        isinstance(datum, (int, numbers.Integral))
        and LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE
        and not isinstance(datum, bool)
    )


def validate_float(datum, **kwargs):
    """
    Check that the data value is a floating
    point number or double precision.

    conditional python types
    (int, float, numbers.Real)

    Parameters
    ----------
    datum: Any
        Data being validated
    kwargs: Any
        Unused kwargs
    """
    return isinstance(datum, (int, float, numbers.Real)) and not isinstance(datum, bool)


def validate_fixed(datum, schema, **kwargs):
    """
    Check that the data value is fixed width bytes,
    matching the schema['size'] exactly!

    Parameters
    ----------
    datum: Any
        Data being validated
    schema: dict
        Schema
    kwargs: Any
        Unused kwargs
    """
    return isinstance(datum, bytes) and len(datum) == schema["size"]


def validate_enum(datum, schema, **kwargs):
    """
    Check that the data value matches one of the enum symbols.

    i.e "blue" in ["red", green", "blue"]

    Parameters
    ----------
    datum: Any
        Data being validated
    schema: dict
        Schema
    kwargs: Any
        Unused kwargs
    """
    return datum in schema["symbols"]


def validate_array(datum, schema, named_schemas, parent_ns=None, raise_errors=True):
    """
    Check that the data list values all match schema['items'].

    Parameters
    ----------
    datum: Any
        Data being validated
    schema: dict
        Schema
    parent_ns: str
        parent namespace
    raise_errors: bool
        If true, raises ValidationError on invalid data
    """
    return (
        isinstance(datum, (Sequence, array.array))
        and not isinstance(datum, str)
        and all(
            _validate(
                datum=d,
                schema=schema["items"],
                named_schemas=named_schemas,
                field=parent_ns,
                raise_errors=raise_errors,
            )
            for d in datum
        )
    )


def validate_map(datum, schema, named_schemas, parent_ns=None, raise_errors=True):
    """
    Check that the data is a Map(k,v)
    matching values to schema['values'] type.

    Parameters
    ----------
    datum: Any
        Data being validated
    schema: dict
        Schema
    parent_ns: str
        parent namespace
    raise_errors: bool
        If true, raises ValidationError on invalid data
    """
    return (
        isinstance(datum, Mapping)
        and all(isinstance(k, str) for k in datum)
        and all(
            _validate(
                datum=v,
                schema=schema["values"],
                named_schemas=named_schemas,
                field=parent_ns,
                raise_errors=raise_errors,
            )
            for v in datum.values()
        )
    )


def validate_record(datum, schema, named_schemas, parent_ns=None, raise_errors=True):
    """
    Check that the data is a Mapping type with all schema defined fields
    validated as True.

    Parameters
    ----------
    datum: Any
        Data being validated
    schema: dict
        Schema
    parent_ns: str
        parent namespace
    raise_errors: bool
        If true, raises ValidationError on invalid data
    """
    _, fullname = schema_name(schema, parent_ns)
    return (
        isinstance(datum, Mapping)
        and not ("-type" in datum and datum["-type"] != fullname)
        and all(
            _validate(
                datum=datum.get(f["name"], f.get("default")),
                schema=f["type"],
                named_schemas=named_schemas,
                field=f"{fullname}.{f['name']}",
                raise_errors=raise_errors,
            )
            for f in schema["fields"]
        )
    )


def validate_union(datum, schema, named_schemas, parent_ns=None, raise_errors=True):
    """
    Check that the data is a list type with possible options to
    validate as True.

    Parameters
    ----------
    datum: Any
        Data being validated
    schema: dict
        Schema
    parent_ns: str
        parent namespace
    raise_errors: bool
        If true, raises ValidationError on invalid data
    """
    if isinstance(datum, tuple):
        (name, datum) = datum
        for candidate in schema:
            if extract_record_type(candidate) == "record":
                schema_name = candidate["name"]
            else:
                schema_name = candidate
            if schema_name == name:
                return _validate(
                    datum,
                    schema=candidate,
                    named_schemas=named_schemas,
                    field=parent_ns,
                    raise_errors=raise_errors,
                )
        else:
            return False

    errors = []
    for s in schema:
        try:
            ret = _validate(
                datum,
                schema=s,
                named_schemas=named_schemas,
                field=parent_ns,
                raise_errors=raise_errors,
            )
            if ret:
                # We exit on the first passing type in Unions
                return True
        except ValidationError as e:
            errors.extend(e.errors)
    if raise_errors:
        raise ValidationError(*errors)
    return False


VALIDATORS = {
    "null": validate_null,
    "boolean": validate_boolean,
    "string": validate_string,
    "int": validate_int,
    "long": validate_long,
    "float": validate_float,
    "double": validate_float,
    "bytes": validate_bytes,
    "fixed": validate_fixed,
    "enum": validate_enum,
    "array": validate_array,
    "map": validate_map,
    "union": validate_union,
    "error_union": validate_union,
    "record": validate_record,
    "error": validate_record,
    "request": validate_record,
}


def _validate(datum, schema, named_schemas, field=None, raise_errors=True):
    # This function expects the schema to already be parsed
    record_type = extract_record_type(schema)
    result = None

    logical_type = extract_logical_type(schema)
    if logical_type:
        prepare = LOGICAL_WRITERS.get(logical_type)
        if prepare:
            datum = prepare(datum, schema)

    validator = VALIDATORS.get(record_type)
    if validator:
        result = validator(
            datum,
            schema=schema,
            named_schemas=named_schemas,
            parent_ns=field,
            raise_errors=raise_errors,
        )
    elif record_type in named_schemas:
        result = _validate(
            datum,
            schema=named_schemas[record_type],
            named_schemas=named_schemas,
            field=field,
            raise_errors=raise_errors,
        )
    else:
        raise UnknownType(record_type)

    if raise_errors and result is False:
        raise ValidationError(ValidationErrorData(datum, schema, field))

    return result


def validate(datum, schema, field=None, raise_errors=True):
    """
    Determine if a python datum is an instance of a schema.

    Parameters
    ----------
    datum: Any
        Data being validated
    schema: dict
        Schema
    field: str, optional
        Record field being validated
    raise_errors: bool, optional
        If true, errors are raised for invalid data. If false, a simple
        True (valid) or False (invalid) result is returned


    Example::

        from fastavro.validation import validate
        schema = {...}
        record = {...}
        validate(record, schema)
    """
    named_schemas = {}
    parsed_schema = parse_schema(schema, named_schemas)
    return _validate(datum, parsed_schema, named_schemas, field, raise_errors)


def validate_many(records, schema, raise_errors=True):
    """
    Validate a list of data!

    Parameters
    ----------
    records: iterable
        List of records to validate
    schema: dict
        Schema
    raise_errors: bool, optional
        If true, errors are raised for invalid data. If false, a simple
        True (valid) or False (invalid) result is returned


    Example::

        from fastavro.validation import validate_many
        schema = {...}
        records = [{...}, {...}, ...]
        validate_many(records, schema)
    """
    named_schemas = {}
    parsed_schema = parse_schema(schema, named_schemas)
    errors = []
    results = []
    for record in records:
        try:
            results.append(
                _validate(
                    record, parsed_schema, named_schemas, raise_errors=raise_errors
                )
            )
        except ValidationError as e:
            errors.extend(e.errors)
    if raise_errors and errors:
        raise ValidationError(*errors)
    return all(results)
