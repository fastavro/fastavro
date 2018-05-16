import datetime
import decimal
import numbers
from collections import Iterable, Mapping

from . import const
from ._six import long, is_str, iterkeys, itervalues
from ._schema import extract_record_type, schema_name
from ._schema_common import SCHEMA_DEFS, UnknownType
from ._validate_common import ValidationError, ValidationErrorData

ctypedef int int32
ctypedef unsigned int uint32
ctypedef unsigned long long ulong64
ctypedef long long long64

cdef int32 INT_MIN_VALUE = const.INT_MIN_VALUE
cdef int32 INT_MAX_VALUE = const.INT_MAX_VALUE
cdef long64 LONG_MIN_VALUE = const.LONG_MIN_VALUE
cdef long64 LONG_MAX_VALUE = const.LONG_MAX_VALUE

cpdef inline bint validate_null(datum, schema=None,
                                str parent_ns='', bint raise_errors=False):
    return datum is None

cpdef inline bint validate_boolean(datum, schema=None,
                                   str parent_ns='', bint raise_errors=False):
    return isinstance(datum, bool)

cpdef inline bint validate_string(datum, schema=None,
                                  str parent_ns='', bint raise_errors=False):
    return is_str(datum)

cpdef inline bint validate_bytes(datum, schema=None,
                                 str parent_ns='', bint raise_errors=False):
    return isinstance(datum, (bytes, decimal.Decimal))

cpdef inline bint validate_int(datum, schema=None,
                               str parent_ns='', bint raise_errors=False):
    return (
        (isinstance(datum, (int, long, numbers.Integral)) and
         INT_MIN_VALUE <= datum <= INT_MAX_VALUE) or
        isinstance(datum, (
            datetime.time, datetime.datetime, datetime.date))
    )

cpdef inline bint validate_long(datum, schema=None,
                                str parent_ns='', bint raise_errors=False):
    return (
        (isinstance(datum, (int, long, numbers.Integral)) and
         LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE) or
        isinstance(datum, (
            datetime.time, datetime.datetime, datetime.date))
    )

cpdef inline bint validate_float(datum, schema=None,
                                 str parent_ns='', bint raise_errors=False):
    return isinstance(datum, (int, long, float, numbers.Real))

cpdef inline bint validate_fixed(datum, dict schema,
                                 str parent_ns='', bint raise_errors=False):
    return (isinstance(datum, bytes) and
            len(datum) == schema['size']) or \
           (isinstance(datum, decimal.Decimal))

cpdef inline bint validate_enum(datum, dict schema,
                                str parent_ns='', bint raise_errors=False):
    return datum in schema['symbols']

cpdef inline bint validate_array(datum, dict schema,
                                 str parent_ns='', bint raise_errors=False):
    if not isinstance(datum, Iterable) or is_str(datum):
        return False

    if raise_errors:
        namespace, name = schema_name(schema, parent_ns)
    else:
        name = parent_ns
    for d in datum:
        if not validate(datum=d, schema=schema['items'],
                        field=name,
                        raise_errors=raise_errors):
            return False
    return True

cpdef inline bint validate_map(object datum, dict schema, str parent_ns='',
                               bint raise_errors=False):
    if raise_errors:
        namespace, name = schema_name(schema, parent_ns)
    else:
        name = parent_ns
    if not isinstance(datum, Mapping):
        return False
    for k in iterkeys(datum):
        if not is_str(k):
            return False
    for v in itervalues(datum):
        if not validate(datum=v, schema=schema['values'],
                        field=name,
                        raise_errors=raise_errors):
            return False
    return True

cpdef inline bint validate_record(object datum, dict schema, str parent_ns='',
                                  bint raise_errors=False):
    if not isinstance(datum, Mapping):
        return False
    if raise_errors:
        namespace, name = schema_name(schema, parent_ns)
    else:
        name = parent_ns
    for f in schema['fields']:
        if not validate(datum=datum.get(f['name'], f.get('default')),
                        schema=f['type'],
                        field=schema_name(f, name)[1] if raise_errors else name,
                        raise_errors=raise_errors):
            return False
    return True

cpdef inline bint validate_union(object datum, list schema, str parent_ns=None,
                                 bint raise_errors=False):
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

BASE_VALIDATORS = {
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

VALIDATORS = BASE_VALIDATORS.copy()

cpdef void register_validator(avro_type, validator):
    if avro_type in BASE_VALIDATORS:
        raise ValueError("Not allowed to override Base Validators.")
    VALIDATORS[avro_type] = validator

cpdef get_validator(avro_type):
    return VALIDATORS.get(avro_type)

cpdef validate(object datum, object schema, str field='',
               bint raise_errors=False):
    """Determine if a python datum is an instance of a schema."""
    record_type = extract_record_type(schema)
    result = None
    ns_field = ''

    if hasattr(schema, 'get') and raise_errors:
        parent_ns, ns_field = schema_name(schema, None)
    elif field:
        ns_field = field

    validator = get_validator(record_type)
    if validator:
        result = validator(datum, schema=schema, parent_ns=ns_field,
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

    return bool(result)
