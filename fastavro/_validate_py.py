import datetime
import decimal
import numbers
from collections import Iterable, Mapping

from ._validate_common import ValidationErrors, ValidationError
from .schema import extract_record_type, schema_name
from .six import long, is_str, iterkeys, itervalues
from ._schema_common import SCHEMA_DEFS, UnknownType

INT_MIN_VALUE = -(1 << 31)
INT_MAX_VALUE = (1 << 31) - 1
LONG_MIN_VALUE = -(1 << 63)
LONG_MAX_VALUE = (1 << 63) - 1


def validate_null(datum, **kwargs):
    return datum is None


def validate_boolean(datum, **kwargs):
    return isinstance(datum, bool)


def validate_string(datum, **kwargs):
    return is_str(datum)


def validate_bytes(datum, **kwargs):
    return isinstance(datum, (bytes, decimal.Decimal))


def validate_int(datum, **kwargs):
    return (
        (isinstance(datum, (int, long, numbers.Integral)) and
         INT_MIN_VALUE <= datum <= INT_MAX_VALUE) or
        isinstance(datum, (datetime.time, datetime.datetime,
                           datetime.date))
    )


def validate_long(datum, **kwargs):
    return (
        (isinstance(datum, (int, long, numbers.Integral)) and
         LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE) or
        isinstance(datum, (datetime.time, datetime.datetime,
                           datetime.date))
    )


def validate_float(datum, **kwargs):
    return isinstance(datum, (int, long, float, numbers.Real))


def validate_fixed(datum, schema, **kwargs):
    return (
        (isinstance(datum, bytes) and len(datum) == schema['size'])
        or (isinstance(datum, decimal.Decimal))
    )


def validate_enum(datum, schema, **kwargs):
    return datum in schema['symbols']


def validate_array(datum, schema, raise_errors=False):
    return (
        isinstance(datum, Iterable) and
        not is_str(datum) and
        all(validate(datum=d, schema=schema['items'],
                     field=schema.get('name'),
                     raise_errors=raise_errors) for d in datum)
    )


def validate_map(datum, schema, raise_errors=False):
    return (
        isinstance(datum, Mapping) and
        all(is_str(k) for k in iterkeys(datum)) and
        all(validate(datum=v, schema=schema['values'],
                     field=schema.get('name'),
                     raise_errors=raise_errors) for v in itervalues(datum))
    )


def validate_record(datum, schema, raise_errors=False):
    return (
        isinstance(datum, Mapping) and
        all(
            validate(datum=datum.get(f['name'], f.get('default')),
                     schema=f['type'],
                     field=schema.get('name'),
                     raise_errors=raise_errors)
            for f in schema['fields']
        )
    )


def validate_union(datum, schema, raise_errors=False):
    if isinstance(datum, tuple):
        (name, datum) = datum
        for candidate in schema:
            if extract_record_type(candidate) == 'record':
                if name == candidate["name"]:
                    return validate(datum, schema=candidate,
                                    field=None,
                                    raise_errors=raise_errors)
        else:
            return False

    errors = []
    for s in schema:
        try:
            ret = validate(datum, schema=s,
                           field=None,
                           raise_errors=raise_errors)
            if ret:
                # We exit on the first passing type in Unions
                return True
        except ValidationErrors as e:
            errors.extend(e.errors)
    if raise_errors:
        raise ValidationErrors(errors)
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


def validate(datum, schema, field=None, parent_ns=None, raise_errors=False):
    """Determine if a python datum is an instance of a schema."""
    record_type = extract_record_type(schema)
    result = None

    if hasattr(schema, 'get'):
        namespace, parent_ns = schema_name(schema, parent_ns)

        if field is not None:
            ns_field = '.'.join([parent_ns, field])
        else:
            ns_field = parent_ns
    else:
        ns_field = '.'.join([parent_ns, field])

    validator = VALIDATORS.get(record_type)
    if validator:
        result = validator(datum, schema=schema, raise_errors=raise_errors)

    if record_type in SCHEMA_DEFS and result is None:
        result = validate(datum,
                          schema=SCHEMA_DEFS[record_type],
                          field=ns_field,
                          raise_errors=raise_errors)

    if raise_errors and result is False:
        raise ValidationErrors(ValidationError(datum, schema, ns_field))

    if result is None:
        raise UnknownType(record_type)

    return result
