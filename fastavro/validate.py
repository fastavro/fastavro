try:
    from fastavro._constants import INT_MIN_VALUE, INT_MAX_VALUE,\
        LONG_MIN_VALUE, LONG_MAX_VALUE
    from fastavro._six import long, is_str, iteritems
    from fastavro._schema import extract_record_type
except ImportError:
    from fastavro.constants import INT_MIN_VALUE, INT_MAX_VALUE,\
        LONG_MIN_VALUE, LONG_MAX_VALUE
    from fastavro.six import long, is_str, iteritems
    from fastavro.schema import extract_record_type

from collections import Iterable, Mapping


def _extract_record_type(schema):
    """Returns the schema type. If it is a record or enum, the namespaced name
    will be returned"""
    _type = extract_record_type(schema)
    if _type in ('record', 'error', 'request', 'enum',):
        namespace = schema.get('namespace')
        name = schema.get('name', _type)
        if namespace:
            _type = '.'.join([namespace, name])
        else:
            _type = name
    return _type


def validate_data(datum, schema, schema_defs):
    """A validator that walks the schema and is able to determine exactly where
    in the schema a problem might exist"""
    err_msg = _validate(datum, schema, schema_defs)
    if err_msg:
        raise ValueError(err_msg)
    else:
        return True


def _validate(datum, schema, schema_defs):
    """Determine if a python datum is an instance of a schema."""

    record_type = extract_record_type(schema)

    if record_type == 'null':
        if datum is not None:
            return u'%s is not None' % type(datum)
        return None

    if record_type == 'boolean':
        if not isinstance(datum, bool):
            return u'%s is not a boolean' % type(datum)
        return None

    if record_type == 'string':
        if not is_str(datum):
            return u'%s is not a string' % type(datum)
        return None

    if record_type == 'bytes':
        if not isinstance(datum, bytes):
            return u'%s is not bytes' % type(datum)
        return None

    if record_type == 'int':
        if not (isinstance(datum, (int, long,)) and
                INT_MIN_VALUE <= datum <= INT_MAX_VALUE):
            return u'%s is not an int' % type(datum)
        return None

    if record_type == 'long':
        if not (isinstance(datum, (int, long,)) and
                LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE):
            return u'%s is not a long' % type(datum)
        return None

    if record_type in ['float', 'double']:
        if not isinstance(datum, (int, long, float)):
            return u'%s is not a float or a double' % type(datum)
        return None

    if record_type == 'fixed':
        if not (isinstance(datum, bytes) and len(datum) == schema['size']):
            return u'%s is not fixed' % type(datum)
        return None

    if record_type == 'union':
        err_msgs = []
        for s in schema:
            err_msg = _validate(datum, s, schema_defs)
            if err_msg:
                err_msgs.append(err_msg)
            else:
                return None

        # If we haven't returned by now, none of the schemas were okay
        s_types = ','.join([_extract_record_type(s) for s in schema])
        union_err = u'%s is not one of %r' % (type(datum), s_types)
        return union_err + '; Potential Reasons:\n   ' + '\n   '.join(err_msgs)

    # dict-y types from here on.
    if record_type == 'enum':
        if datum not in schema['symbols']:
            msg = u'<%s[%s]>: %s not in symbols %s'
            return msg % (_extract_record_type(schema),
                          datum,
                          datum,
                          schema['symbols'])

        return None

    if record_type == 'array':
        if not isinstance(datum, Iterable):
            return u'%s is not iterable' % type(datum)

        for idx, d in enumerate(datum):
            err_msg = _validate(d, schema['items'], schema_defs)
            if err_msg:
                return u'<array[%d]>: %s' % (idx, err_msg)

        return None

    if record_type == 'map':
        if not isinstance(datum, Mapping):
            return u'%s is not a map' % type(datum)

        for k, v in iteritems(datum):
            if not is_str(k):
                return u'<map[%r]>: Invalid key' % k

            err_msg = _validate(v, schema['values'], schema_defs)
            if err_msg:
                return u'<map[%s]>: %s' % (k, err_msg)

        return None

    if record_type in ('record', 'error', 'request',):
        if not isinstance(datum, Mapping):
            return u'%s is not a record dictionary' % type(datum)

        for f in schema['fields']:
            err_msg = _validate(datum.get(f['name']), f['type'], schema_defs)
            if err_msg:
                return u'<%s><field[%s]>: %s' % (_extract_record_type(schema),
                                                 f['name'],
                                                 err_msg)

        return None

    if record_type in schema_defs:
        return _validate(datum, schema_defs[record_type], schema_defs)

    raise ValueError('unkown record type - %s' % record_type)
