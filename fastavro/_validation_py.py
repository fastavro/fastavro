import datetime
import decimal
import numbers
from collections import Iterable, Mapping

from fastavro.const import (
    INT_MAX_VALUE, INT_MIN_VALUE, LONG_MAX_VALUE, LONG_MIN_VALUE
)
from ._validate_common import ValidationError, ValidationErrorData
from .schema import extract_record_type, schema_name, UnknownType
from .six import long, is_str, iterkeys, itervalues
from ._schema_common import SCHEMA_DEFS


def validate_null(datum, **kwargs):
    """
    Checks that the data value is None.

    :param datum: None : data to validate as None
    :param kwargs: black-hole args
    :return: bool
    """
    return datum is None


def validate_boolean(datum, **kwargs):
    """
    Check that the data value is bool instance

    :param datum: (bool) : data to validate as boolean
    :param kwargs: black-hole args
    :return: bool
    """
    return isinstance(datum, bool)


def validate_string(datum, **kwargs):
    """
    Check that the data value is string type, uses
    six for Python version compatibility.

    :param datum: (str, basestring, unicode) : data to validate as string
    :param kwargs: black-hole args
    :return: bool
    """
    return is_str(datum)


def validate_bytes(datum, **kwargs):
    """
    Check that the data value is (python bytes type or decimal.Decimal type

    :param datum: (bytes, decimal.Decimal): data to validate as bytes
    :param kwargs: black-hole args
    :return: bool
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

    :param datum: number: data to validate as int32
    :param kwargs: black-hole args
    :return: bool
    """
    return (
            (isinstance(datum, (int, long, numbers.Integral)) and
             INT_MIN_VALUE <= datum <= INT_MAX_VALUE) or
            isinstance(datum, (datetime.time, datetime.datetime,
                               datetime.date))
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

    :param datum: number: data to validate as long64
    :param kwargs: black-hole args
    :return: bool
    """
    return (
            (isinstance(datum, (int, long, numbers.Integral)) and
             LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE) or
            isinstance(datum, (datetime.time, datetime.datetime,
                               datetime.date))
    )


def validate_float(datum, **kwargs):
    """
    Check that the data value is a floating
    point number or double precision.

    conditional python types
    (int, long, float, numbers.Real)

    :param datum: number: data to validate as float
    :param kwargs: black-hole args
    :return: bool
    """
    return isinstance(datum, (int, long, float, numbers.Real))


def validate_fixed(datum, schema, **kwargs):
    """
    Check that the data value is fixed width bytes,
    matching the schema['size'] exactly!

    :param datum: (bytes, decimal.Decimal): data to validate as fixed bytes
    :param schema: avro schema of 'fixed' type
    :param kwargs: black-hole args
    :return: bool
    """
    return (
            (isinstance(datum, bytes) and len(datum) == schema['size'])
            or (isinstance(datum, decimal.Decimal))
    )


def validate_enum(datum, schema, **kwargs):
    """
    Check that the data value matches one of the enum symbols.

    i.e "blue" in ["red", green", "blue"]

    :param datum: str: data to validate in enum symbols
    :param schema: avro schema of 'enum' type
    :param kwargs: black-hole args
    :return: bool
    """
    return datum in schema['symbols']


def validate_array(datum, schema, parent_ns=None, raise_errors=True):
    """
    Check that the data list values all match schema['items'].

    :param datum: list: data to validate as specified "items" type
    :param schema: avro schema of 'array' type
    :param parent_ns: str: parent namespace
    :param raise_errors: bool: should raise ValidationError
    :return: bool
    :except: ValidationError
    """
    if raise_errors:
        _, name = schema_name(schema, parent_ns)
    else:
        name = parent_ns
    return (
            isinstance(datum, Iterable) and
            not is_str(datum) and
            all(validate(datum=d, schema=schema['items'],
                         field=name,
                         raise_errors=raise_errors) for d in datum)
    )


def validate_map(datum, schema, parent_ns=None, raise_errors=True):
    """
    Check that the data is a Map(k,v)
    matching values to schema['values'] type.

    :param datum: Mapping: data to validate as specified "items" type
    :param schema: avro schema of 'map' type
    :param parent_ns: str: parent namespace
    :param raise_errors: bool: should raise ValidationError
    :return: bool
    :except: ValidationError
    """
    if raise_errors:
        _, name = schema_name(schema, parent_ns)
    else:
        name = parent_ns
    return (
            isinstance(datum, Mapping) and
            all(is_str(k) for k in iterkeys(datum)) and
            all(validate(datum=v, schema=schema['values'],
                         field=name,
                         raise_errors=raise_errors) for v in itervalues(datum))
    )


def validate_record(datum, schema, parent_ns=None, raise_errors=True):
    """
    Check that the data is a Mapping type with all schema defined fields
    validated as True.

    :param datum: Mapping: data to validate schema fields
    :param schema: avro schema of 'record' type
    :param parent_ns: str: parent namespace
    :param raise_errors: bool: should raise ValidationError
    :return: bool
    :except: ValidationError
    """
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


def validate_union(datum, schema, parent_ns=None, raise_errors=True):
    """
    Check that the data is a list type with possible options to
    validate as True.

    :param datum: (Iterable, tuple(name, Iterable)): data to validate
    as multiple data types
    :param schema: avro schema of 'union' type
    :param parent_ns: str: parent namespace
    :param raise_errors: bool: should raise ValidationError
    :return: bool
    :except: ValidationError
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
    """Determine if a python datum is an instance of a schema."""
    record_type = extract_record_type(schema)
    result = None
    ns_field = ''

    if hasattr(schema, 'get') and raise_errors:
        parent_ns, ns_field = schema_name(schema, None)
    elif field:
        ns_field = field

    validator = VALIDATORS.get(record_type)
    if validator:
        result = validator(datum, schema=schema,
                           parent_ns=ns_field,
                           raise_errors=raise_errors)
    elif record_type in SCHEMA_DEFS:
        result = validate(datum,
                          schema=SCHEMA_DEFS[record_type],
                          field=ns_field,
                          raise_errors=raise_errors)
    else:
        raise UnknownType(record_type)

    if raise_errors and result is False:
        raise ValidationError(ValidationErrorData(datum, schema, ns_field))

    return result


def validate_many(records, schema, raise_errors=True):
    """
    Validate a list of data!

    :param records: Iterable: list of records to validate
    :param schema: Avro schema
    :param raise_errors: bool: should raise ValidationError
    :return: bool
    :except: ValidationError
    """
    errors = []
    results = []
    for record in records:
        try:
            results.append(validate(record, schema, raise_errors=raise_errors))
        except ValidationError as e:
            errors.extend(e.errors)
    if raise_errors:
        raise ValidationError(*errors)
    return all(results)
