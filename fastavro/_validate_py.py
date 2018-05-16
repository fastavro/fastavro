import datetime
import decimal
import numbers
from collections import Iterable, Mapping

from fastavro.const import INT_MAX_VALUE, INT_MIN_VALUE, \
    LONG_MAX_VALUE, LONG_MIN_VALUE
from ._validate_common import ValidationError, ValidationErrorData
from .schema import extract_record_type, schema_name, UnknownType
from .six import long, is_str, iterkeys, itervalues
from ._schema_common import SCHEMA_DEFS


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


def validate_array(datum, schema, parent_ns=None, raise_errors=False):
    if raise_errors:
        namespace, name = schema_name(schema, parent_ns)
    else:
        name = parent_ns
    return (
            isinstance(datum, Iterable) and
            not is_str(datum) and
            all(validate(datum=d, schema=schema['items'],
                         field=name,
                         raise_errors=raise_errors) for d in datum)
    )


def validate_map(datum, schema, parent_ns=None, raise_errors=False):
    if raise_errors:
        namespace, name = schema_name(schema, parent_ns)
    else:
        name = parent_ns
    return (
            isinstance(datum, Mapping) and
            all(is_str(k) for k in iterkeys(datum)) and
            all(validate(datum=v, schema=schema['values'],
                         field=name,
                         raise_errors=raise_errors) for v in itervalues(datum))
    )


def validate_record(datum, schema, parent_ns=None, raise_errors=False):
    if raise_errors:
        namespace, name = schema_name(schema, parent_ns)
    else:
        name = parent_ns
    return (
        isinstance(datum, Mapping) and
        all(validate(datum=datum.get(f['name'], f.get('default')),
                     schema=f['type'],
                     field=schema_name(f, name)[1] if raise_errors else name,
                     raise_errors=raise_errors)
            for f in schema['fields']
            )
    )


def validate_union(datum, schema, parent_ns=None, raise_errors=False):
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


def validate(datum, schema, field=None, raise_errors=False):
    """Determine if a python datum is an instance of a schema."""
    record_type = extract_record_type(schema)
    result = None
    ns_field = ''

    if hasattr(schema, 'get') and raise_errors:
        parent_ns, ns_field = schema_name(schema, None)
    elif field:
        ns_field = field

    if record_type in ('union', 'null'):
        # test_string_not_treated_as_array
        validator = VALIDATORS.get(record_type)
        result = validator(datum, schema=schema, parent_ns=ns_field,
                           raise_errors=raise_errors)
    elif datum is None:
        result = False
    else:
        validator = VALIDATORS.get(record_type)
        if validator:
            result = validator(datum, schema=schema,
                               parent_ns=ns_field,
                               raise_errors=raise_errors)

    if record_type in SCHEMA_DEFS and result is None:
        result = validate(datum,
                          schema=SCHEMA_DEFS[record_type],
                          field=ns_field,
                          raise_errors=raise_errors)

    if raise_errors and result is False:
        raise ValidationError(ValidationErrorData(datum, schema, ns_field))

    if result is None:
        raise UnknownType(record_type)

    return result
