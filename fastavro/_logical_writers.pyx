# cython: language_level=3str

import datetime

import decimal
import os
import uuid

from libc.time cimport tm, mktime
from cpython.int cimport PyInt_AS_LONG
from cpython.tuple cimport PyTuple_GET_ITEM

from fastavro import const
from ._six import long, mk_bits, int_to_be_signed_bytes
from ._timezone import epoch, epoch_naive

ctypedef long long long64

cdef long64 MCS_PER_SECOND = const.MCS_PER_SECOND
cdef long64 MCS_PER_MINUTE = const.MCS_PER_MINUTE
cdef long64 MCS_PER_HOUR = const.MCS_PER_HOUR

cdef long64 MLS_PER_SECOND = const.MLS_PER_SECOND
cdef long64 MLS_PER_MINUTE = const.MLS_PER_MINUTE
cdef long64 MLS_PER_HOUR = const.MLS_PER_HOUR

cdef is_windows = os.name == 'nt'

# The function datetime.timestamp() is a simpler, faster way to convert a
# datetime to a Unix timestamp, but is only available in Python 3.3 and later.
cdef has_timestamp_fn = hasattr(datetime.datetime, 'timestamp')


cpdef prepare_timestamp_millis(object data, schema):
    cdef object tt
    cdef tm time_tuple
    if isinstance(data, datetime.datetime):
        if not has_timestamp_fn:
            if data.tzinfo is not None:
                return <long64>(<double>(
                    <object>(data - epoch).total_seconds()) * MLS_PER_SECOND
                )
            tt = data.timetuple()
            time_tuple.tm_sec = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 5)))
            time_tuple.tm_min = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 4)))
            time_tuple.tm_hour = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 3)))
            time_tuple.tm_mday = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 2)))
            time_tuple.tm_mon = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 1))) - 1
            time_tuple.tm_year = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 0))) - 1900
            time_tuple.tm_isdst = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 8)))

            return mktime(& time_tuple) * MLS_PER_SECOND + <long64>(
                int(data.microsecond) / 1000)
        else:
            # On Windows, timestamps before the epoch will raise an error.
            # See https://bugs.python.org/issue36439
            if is_windows:
                if data.tzinfo is not None:
                    return <long64>(<double>(
                        <object>(data - epoch).total_seconds()) * MLS_PER_SECOND
                    )
                else:
                    return <long64>(<double>(
                        <object>(data - epoch_naive).total_seconds()) * MLS_PER_SECOND
                    )
            else:
                return <long64>(<double>(data.timestamp()) * MLS_PER_SECOND)
    else:
        return data


cpdef prepare_timestamp_micros(object data, schema):
    cdef object tt
    cdef tm time_tuple
    if isinstance(data, datetime.datetime):
        if not has_timestamp_fn:
            if data.tzinfo is not None:
                return <long64>(<double>(
                    <object>(data - epoch).total_seconds()) * MCS_PER_SECOND
                )
            tt = data.timetuple()
            time_tuple.tm_sec = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 5)))
            time_tuple.tm_min = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 4)))
            time_tuple.tm_hour = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 3)))
            time_tuple.tm_mday = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 2)))
            time_tuple.tm_mon = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 1))) - 1
            time_tuple.tm_year = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 0))) - 1900
            time_tuple.tm_isdst = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 8)))

            return mktime(& time_tuple) * MCS_PER_SECOND + \
                <long64>(data.microsecond)
        else:
            # On Windows, timestamps before the epoch will raise an error.
            # See https://bugs.python.org/issue36439
            if is_windows:
                if data.tzinfo is not None:
                    return <long64>(<double>(
                        <object>(data - epoch).total_seconds()) * MCS_PER_SECOND
                    )
                else:
                    return <long64>(<double>(
                        <object>(data - epoch_naive).total_seconds()) * MCS_PER_SECOND
                    )
            else:
                return <long64>(<double>(data.timestamp()) * MCS_PER_SECOND)
    else:
        return data


cpdef prepare_date(object data, schema):
    if isinstance(data, datetime.date):
        return data.toordinal() - const.DAYS_SHIFT
    elif isinstance(data, str):
        return datetime.datetime.strptime(data, "%Y-%m-%d").toordinal() - const.DAYS_SHIFT
    else:
        return data


cpdef prepare_bytes_decimal(object data, schema):
    """Convert decimal.Decimal to bytes"""
    if not isinstance(data, decimal.Decimal):
        return data
    scale = schema.get('scale', 0)

    sign, digits, exp = data.as_tuple()

    delta = exp + scale

    if delta < 0:
        raise ValueError(
            'Scale provided in schema does not match the decimal')

    unscaled_datum = 0
    for digit in digits:
        unscaled_datum = (unscaled_datum * 10) + digit

    unscaled_datum = 10 ** delta * unscaled_datum

    bytes_req = (unscaled_datum.bit_length() + 8) // 8

    if sign:
        unscaled_datum = -unscaled_datum

    return int_to_be_signed_bytes(unscaled_datum, bytes_req)


cpdef prepare_fixed_decimal(object data, schema):
    cdef bytearray tmp
    if not isinstance(data, decimal.Decimal):
        return data
    scale = schema.get('scale', 0)
    size = schema['size']

    # based on https://github.com/apache/avro/pull/82/

    sign, digits, exp = data.as_tuple()

    if -exp > scale:
        raise ValueError(
            'Scale provided in schema does not match the decimal')
    delta = exp + scale
    if delta > 0:
        digits = digits + (0,) * delta

    unscaled_datum = 0
    for digit in digits:
        unscaled_datum = (unscaled_datum * 10) + digit

    bits_req = unscaled_datum.bit_length() + 1

    size_in_bits = size * 8
    offset_bits = size_in_bits - bits_req

    mask = 2 ** size_in_bits - 1
    bit = 1
    for i in range(bits_req):
        mask ^= bit
        bit <<= 1

    if bits_req < 8:
        bytes_req = 1
    else:
        bytes_req = bits_req // 8
        if bits_req % 8 != 0:
            bytes_req += 1

    tmp = bytearray()

    if sign:
        unscaled_datum = (1 << bits_req) - unscaled_datum
        unscaled_datum = mask | unscaled_datum
        for index in range(size - 1, -1, -1):
            bits_to_write = unscaled_datum >> (8 * index)
            tmp += mk_bits(bits_to_write & 0xff)
    else:
        for i in range(offset_bits // 8):
            tmp += mk_bits(0)
        for index in range(bytes_req - 1, -1, -1):
            bits_to_write = unscaled_datum >> (8 * index)
            tmp += mk_bits(bits_to_write & 0xff)

    return tmp


cpdef prepare_uuid(object data, schema):
    if isinstance(data, uuid.UUID):
        return str(data)
    else:
        return data


cpdef prepare_time_millis(object data, schema):
    if isinstance(data, datetime.time):
        return int(
            data.hour * MLS_PER_HOUR + data.minute * MLS_PER_MINUTE
            + data.second * MLS_PER_SECOND + int(data.microsecond / 1000))
    else:
        return data


cpdef prepare_time_micros(object data, schema):
    if isinstance(data, datetime.time):
        return long(data.hour * MCS_PER_HOUR + data.minute * MCS_PER_MINUTE
                    + data.second * MCS_PER_SECOND + data.microsecond)
    else:
        return data


LOGICAL_WRITERS = {
    'long-timestamp-millis': prepare_timestamp_millis,
    'long-timestamp-micros': prepare_timestamp_micros,
    'int-date': prepare_date,
    'bytes-decimal': prepare_bytes_decimal,
    'fixed-decimal': prepare_fixed_decimal,
    'string-uuid': prepare_uuid,
    'int-time-millis': prepare_time_millis,
    'long-time-micros': prepare_time_micros,

}
