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

cdef inline bint validate_null(datum, schema=None,
                               str parent_ns='', bint raise_errors=True):
    return datum is None


cdef inline bint validate_boolean(datum, schema=None,
                                  str parent_ns='', bint raise_errors=True):
    return isinstance(datum, bool)


cdef inline bint validate_string(datum, schema=None,
                                 str parent_ns='', bint raise_errors=True):
    return is_str(datum)


cdef inline bint validate_bytes(datum, schema=None,
                                str parent_ns='', bint raise_errors=True):
    return isinstance(datum, (bytes, decimal.Decimal))


cdef inline bint validate_int(datum, schema=None,
                              str parent_ns='', bint raise_errors=True):
    return (
        (isinstance(datum, (int, long, numbers.Integral)) and
         INT_MIN_VALUE <= datum <= INT_MAX_VALUE) or
        isinstance(datum, (
            datetime.time, datetime.datetime, datetime.date))
    )


cdef inline bint validate_long(datum, schema=None,
                               str parent_ns='', bint raise_errors=True):
    return (
        (isinstance(datum, (int, long, numbers.Integral)) and
         LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE) or
        isinstance(datum, (
            datetime.time, datetime.datetime, datetime.date))
    )


cdef inline bint validate_float(datum, schema=None,
                                str parent_ns='', bint raise_errors=True):
    return isinstance(datum, (int, long, float, numbers.Real))


cdef inline bint validate_fixed(datum, dict schema,
                                str parent_ns='', bint raise_errors=True):
    return (isinstance(datum, bytes) and
            len(datum) == schema['size']) or \
           (isinstance(datum, decimal.Decimal))


cdef inline bint validate_enum(datum, dict schema,
                               str parent_ns='', bint raise_errors=True):
    return datum in schema['symbols']


cdef inline bint validate_array(datum, dict schema,
                                str parent_ns='', bint raise_errors=True) except -1:
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


cdef inline bint validate_map(object datum, dict schema, str parent_ns='',
                              bint raise_errors=True) except -1:
    # initial checks for map type
    if not isinstance(datum, Mapping):
        return False
    for k in iterkeys(datum):
        if not is_str(k):
            return False

    if raise_errors:
        namespace, name = schema_name(schema, parent_ns)
    else:
        name = parent_ns
    for v in itervalues(datum):
        if not validate(datum=v, schema=schema['values'],
                        field=name,
                        raise_errors=raise_errors):
            return False
    return True


cdef inline bint validate_record(object datum, dict schema, str parent_ns='',
                                 bint raise_errors=True) except -1:
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
    ns_field = ''

    if hasattr(schema, 'get') and raise_errors:
        parent_ns, ns_field = schema_name(schema, None)
    elif field:
        ns_field = field

    # explicit, so that cython is faster, but only for Base Validators
    if record_type == 'null':
        result = validate_null(datum, schema=schema, parent_ns=ns_field,
                               raise_errors=raise_errors)
    elif record_type == 'boolean':
        result = validate_boolean(datum, schema=schema, parent_ns=ns_field,
                                  raise_errors=raise_errors)
    elif record_type == 'string':
        result = validate_string(datum, schema=schema, parent_ns=ns_field,
                                 raise_errors=raise_errors)
    elif record_type == 'int':
        result = validate_int(datum, schema=schema, parent_ns=ns_field,
                              raise_errors=raise_errors)
    elif record_type == 'long':
        result = validate_long(datum, schema=schema, parent_ns=ns_field,
                               raise_errors=raise_errors)
    elif record_type in ('float', 'double'):
        result = validate_float(datum, schema=schema, parent_ns=ns_field,
                                raise_errors=raise_errors)
    elif record_type == 'bytes':
        result = validate_bytes(datum, schema=schema, parent_ns=ns_field,
                                raise_errors=raise_errors)
    elif record_type == 'fixed':
        result = validate_fixed(datum, schema=schema, parent_ns=ns_field,
                                raise_errors=raise_errors)
    elif record_type == 'enum':
        result = validate_enum(datum, schema=schema, parent_ns=ns_field,
                               raise_errors=raise_errors)
    elif record_type == 'array':
        result = validate_array(datum, schema=schema, parent_ns=ns_field,
                                raise_errors=raise_errors)
    elif record_type == 'map':
        result = validate_map(datum, schema=schema, parent_ns=ns_field,
                              raise_errors=raise_errors)
    elif record_type in ('union', 'error_union'):
        result = validate_union(datum, schema=schema, parent_ns=ns_field,
                                raise_errors=raise_errors)
    elif record_type in ('record', 'error', 'request'):
        result = validate_record(datum, schema=schema, parent_ns=ns_field,
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
    if raise_errors:
        raise ValidationError(*errors)
    return all(results)
