# cython: language_level=3str

import datetime
import decimal
import numbers
from uuid import UUID
try:
    from collections.abc import Mapping, Sequence
except ImportError:
    # python2
    from collections import Mapping, Sequence

from . import const
from ._six import long, is_str, iterkeys, itervalues
from ._schema import extract_record_type, extract_logical_type, schema_name
from ._logical_writers import LOGICAL_WRITERS
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


cdef inline bint validate_null(datum, schema=None,
                               str parent_ns='', bint raise_errors=True):
    return datum is None


cdef inline bint validate_boolean(datum, schema=None,
                                  str parent_ns='', bint raise_errors=True):
    return isinstance(datum, bool)


cdef inline bint validate_string(datum, schema=None,
                                 str parent_ns='', bint raise_errors=True):
    return is_str(datum) or isinstance(datum, UUID)


cdef inline bint validate_bytes(datum, schema=None,
                                str parent_ns='', bint raise_errors=True):
    return isinstance(datum, (bytes, decimal.Decimal))


cdef inline bint validate_int(datum, schema=None,
                              str parent_ns='', bint raise_errors=True):
    return (
        (isinstance(datum, (int, long, numbers.Integral))
         and INT_MIN_VALUE <= datum <= INT_MAX_VALUE
         and not isinstance(datum, bool))
        or isinstance(datum, (
            datetime.time, datetime.datetime, datetime.date))
    )


cdef inline bint validate_long(datum, schema=None,
                               str parent_ns='', bint raise_errors=True):
    return (
        (isinstance(datum, (int, long, numbers.Integral))
         and LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE
         and not isinstance(datum, bool))
        or isinstance(datum, (datetime.time, datetime.datetime, datetime.date))
    )


cdef inline bint validate_float(datum, schema=None,
                                str parent_ns='', bint raise_errors=True):
    return (
        isinstance(datum, (int, long, float, numbers.Real))
        and not isinstance(datum, bool)
    )


cdef inline bint validate_fixed(datum, dict schema,
                                str parent_ns='', bint raise_errors=True):
    return (
        ((isinstance(datum, bytes) or isinstance(datum, bytearray))
         and len(datum) == schema['size'])
        or isinstance(datum, decimal.Decimal)
    )


cdef inline bint validate_enum(datum, dict schema,
                               str parent_ns='', bint raise_errors=True):
    return datum in schema['symbols']


cdef inline bint validate_array(datum, dict schema,
                                str parent_ns='', bint raise_errors=True) except -1:
    if not isinstance(datum, Sequence) or is_str(datum):
        return False

    for d in datum:
        if not validate(datum=d, schema=schema['items'],
                        field=parent_ns,
                        raise_errors=raise_errors):
            return False
    return True


cdef inline bint validate_map(object datum, dict schema, str parent_ns='',
                              bint raise_errors=True) except -1:
    # initial checks for map type
    if not isinstance(datum, Mapping):
        return False
    for k in iterkeys(datum):
        if not is_str(k):
            return False

    for v in itervalues(datum):
        if not validate(datum=v, schema=schema['values'],
                        field=parent_ns,
                        raise_errors=raise_errors):
            return False
    return True


cdef inline bint validate_record(object datum, dict schema, str parent_ns='',
                                 bint raise_errors=True) except -1:
    if not isinstance(datum, Mapping):
        return False
    _, namespace = schema_name(schema, parent_ns)
    for f in schema['fields']:
        if not validate(datum=datum.get(f['name'], f.get('default')),
                        schema=f['type'],
                        field='{}.{}'.format(namespace, f['name']),
                        raise_errors=raise_errors):
            return False
    return True


cdef inline bint validate_union(object datum, list schema, str parent_ns=None,
                                bint raise_errors=True) except -1:
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

    cdef list errors = []
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


cpdef validate(object datum, object schema, str field='',
               bint raise_errors=True):
    record_type = extract_record_type(schema)
    result = None

    logical_type = extract_logical_type(schema)
    if logical_type:
        prepare = LOGICAL_WRITERS.get(logical_type)
        if prepare:
            datum = prepare(datum, schema)

    # explicit, so that cython is faster, but only for Base Validators
    if record_type == 'null':
        result = validate_null(datum, schema=schema, parent_ns=field,
                               raise_errors=raise_errors)
    elif record_type == 'boolean':
        result = validate_boolean(datum, schema=schema, parent_ns=field,
                                  raise_errors=raise_errors)
    elif record_type == 'string':
        result = validate_string(datum, schema=schema, parent_ns=field,
                                 raise_errors=raise_errors)
    elif record_type == 'int':
        result = validate_int(datum, schema=schema, parent_ns=field,
                              raise_errors=raise_errors)
    elif record_type == 'long':
        result = validate_long(datum, schema=schema, parent_ns=field,
                               raise_errors=raise_errors)
    elif record_type in ('float', 'double'):
        result = validate_float(datum, schema=schema, parent_ns=field,
                                raise_errors=raise_errors)
    elif record_type == 'bytes':
        result = validate_bytes(datum, schema=schema, parent_ns=field,
                                raise_errors=raise_errors)
    elif record_type == 'fixed':
        result = validate_fixed(datum, schema=schema, parent_ns=field,
                                raise_errors=raise_errors)
    elif record_type == 'enum':
        result = validate_enum(datum, schema=schema, parent_ns=field,
                               raise_errors=raise_errors)
    elif record_type == 'array':
        result = validate_array(datum, schema=schema, parent_ns=field,
                                raise_errors=raise_errors)
    elif record_type == 'map':
        result = validate_map(datum, schema=schema, parent_ns=field,
                              raise_errors=raise_errors)
    elif record_type in ('union', 'error_union'):
        result = validate_union(datum, schema=schema, parent_ns=field,
                                raise_errors=raise_errors)
    elif record_type in ('record', 'error', 'request'):
        result = validate_record(datum, schema=schema, parent_ns=field,
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

    return bool(result)


cpdef validate_many(records, schema, bint raise_errors=True):
    cdef bint result
    cdef list errors = []
    cdef list results = []
    for record in records:
        try:
            result = validate(record, schema, raise_errors=raise_errors)
            results.append(result)
        except ValidationError as e:
            errors.extend(e.errors)
    if raise_errors and errors:
        raise ValidationError(*errors)
    return all(results)
