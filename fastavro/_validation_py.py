import datetime
import decimal
import numbers
from uuid import UUID
try:
    from collections.abc import Mapping, Sequence
except ImportError:
    # python2
    from collections import Mapping, Sequence

from fastavro.const import (
    INT_MAX_VALUE, INT_MIN_VALUE, LONG_MAX_VALUE, LONG_MIN_VALUE
)
from ._validate_common import ValidationError, ValidationErrorData
from .schema import extract_record_type, UnknownType, schema_name
from .six import long, is_str, iterkeys, itervalues
from ._schema_common import SCHEMA_DEFS


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
    Check that the data value is string or UUID type, uses
    six for Python version compatibility.

    Parameters
    ----------
    datum: Any
        Data being validated
    kwargs: Any
        Unused kwargs
    """
    return is_str(datum) or isinstance(datum, UUID)


def validate_bytes(datum, **kwargs):
    """
    Check that the data value is (python bytes type or decimal.Decimal type

    Parameters
    ----------
    datum: Any
        Data being validated
    kwargs: Any
        Unused kwargs
    """
    return isinstance(datum, (bytes, decimal.Decimal))


def validate_int(datum, **kwargs):
    """
    Check that the data value is a non floating
    point number with size less that Int32.
    Also support for logicalType timestamp validation with datetime.

    Int32 = -2147483648<=datum<=2147483647

    conditional python types
    (int, long, numbers.Integral,
    datetime.time, datetime.datetime, datetime.date)

    Parameters
    ----------
    datum: Any
        Data being validated
    kwargs: Any
        Unused kwargs
    """
    return (
            (isinstance(datum, (int, long, numbers.Integral))
             and INT_MIN_VALUE <= datum <= INT_MAX_VALUE
             and not isinstance(datum, bool))
            or isinstance(
                datum, (datetime.time, datetime.datetime, datetime.date)
            )
    )


def validate_long(datum, **kwargs):
    """
    Check that the data value is a non floating
    point number with size less that long64.
    * Also support for logicalType timestamp validation with datetime.

    Int64 = -9223372036854775808 <= datum <= 9223372036854775807

    conditional python types
    (int, long, numbers.Integral,
    datetime.time, datetime.datetime, datetime.date)

    :Parameters
    ----------
    datum: Any
        Data being validated
    kwargs: Any
        Unused kwargs
    """
    return (
            (isinstance(datum, (int, long, numbers.Integral))
             and LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE
             and not isinstance(datum, bool))
            or isinstance(
                datum, (datetime.time, datetime.datetime, datetime.date)
            )
    )


def validate_float(datum, **kwargs):
    """
    Check that the data value is a floating
    point number or double precision.

    conditional python types
    (int, long, float, numbers.Real)

    Parameters
    ----------
    datum: Any
        Data being validated
    kwargs: Any
        Unused kwargs
    """
    return (
        isinstance(datum, (int, long, float, numbers.Real))
        and not isinstance(datum, bool)
    )


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
    return (
            (isinstance(datum, bytes) and len(datum) == schema['size'])
            or (isinstance(datum, decimal.Decimal))
    )


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
    return datum in schema['symbols']


def validate_array(datum, schema, parent_ns=None, raise_errors=True):
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
            isinstance(datum, Sequence) and
            not is_str(datum) and
            all(validate(datum=d, schema=schema['items'],
                         field=parent_ns,
                         raise_errors=raise_errors) for d in datum)
    )


def validate_map(datum, schema, parent_ns=None, raise_errors=True):
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
            isinstance(datum, Mapping) and
            all(is_str(k) for k in iterkeys(datum)) and
            all(validate(datum=v, schema=schema['values'],
                         field=parent_ns,
                         raise_errors=raise_errors) for v in itervalues(datum))
    )


def validate_record(datum, schema, parent_ns=None, raise_errors=True):
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
    _, namespace = schema_name(schema, parent_ns)
    return (
        isinstance(datum, Mapping) and
        all(validate(datum=datum.get(f['name'], f.get('default')),
                     schema=f['type'],
                     field='{}.{}'.format(namespace, f['name']),
                     raise_errors=raise_errors)
            for f in schema['fields']
            )
    )


def validate_union(datum, schema, parent_ns=None, raise_errors=True):
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
            if extract_record_type(candidate) == 'record':
                if name == candidate["name"]:
                    return validate(datum, schema=candidate,
                                    field=parent_ns,
                                    raise_errors=raise_errors)
        else:
            return False

    errors = []
    for s in schema:
        try:
            ret = validate(datum, schema=s,
                           field=parent_ns,
                           raise_errors=raise_errors)
            if ret:
                # We exit on the first passing type in Unions
                return True
        except ValidationError as e:
            errors.extend(e.errors)
    if raise_errors:
        raise ValidationError(*errors)
    return False


VALIDATORS = {
    'null': validate_null,
    'boolean': validate_boolean,
    'string': validate_string,
    'int': validate_int,
    'long': validate_long,
    'float': validate_float,
    'double': validate_float,
    'bytes': validate_bytes,
    'fixed': validate_fixed,
    'enum': validate_enum,
    'array': validate_array,
    'map': validate_map,
    'union': validate_union,
    'error_union': validate_union,
    'record': validate_record,
    'error': validate_record,
    'request': validate_record
}


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
    record_type = extract_record_type(schema)
    result = None

    validator = VALIDATORS.get(record_type)
    if validator:
        result = validator(datum, schema=schema,
                           parent_ns=field,
                           raise_errors=raise_errors)
    elif record_type in SCHEMA_DEFS:
        result = validate(datum,
                          schema=SCHEMA_DEFS[record_type],
                          field=field,
                          raise_errors=raise_errors)
    else:
        raise UnknownType(record_type)

    if raise_errors and result is False:
        raise ValidationError(ValidationErrorData(datum, schema, field))

    return result


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
    errors = []
    results = []
    for record in records:
        try:
            results.append(validate(record, schema, raise_errors=raise_errors))
        except ValidationError as e:
            errors.extend(e.errors)
    if raise_errors and errors:
        raise ValidationError(*errors)
    return all(results)
