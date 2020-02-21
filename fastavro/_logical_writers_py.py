# cython: auto_cpdef=True

import datetime
import decimal
import os
import time
import uuid
from .const import (
    MCS_PER_HOUR, MCS_PER_MINUTE, MCS_PER_SECOND, MLS_PER_HOUR, MLS_PER_MINUTE,
    MLS_PER_SECOND, DAYS_SHIFT
)
from .six import MemoryIO, long, mk_bits, int_to_be_signed_bytes
from ._timezone import epoch, epoch_naive


is_windows = os.name == 'nt'


def prepare_timestamp_millis(data, schema):
    """Converts datetime.datetime object to int timestamp with milliseconds
    """
    if isinstance(data, datetime.datetime):
        if data.tzinfo is not None:
            delta = (data - epoch)
            return int(delta.total_seconds() * MLS_PER_SECOND)

        # On Windows, mktime does not support pre-epoch, see e.g.
        # https://stackoverflow.com/questions/2518706/python-mktime-overflow-error
        if is_windows:
            delta = (data - epoch_naive)
            return int(delta.total_seconds() * MLS_PER_SECOND)
        else:
            t = int(time.mktime(data.timetuple())) * MLS_PER_SECOND + int(
                data.microsecond / 1000)
            return t
    else:
        return data


def prepare_timestamp_micros(data, schema):
    """Converts datetime.datetime to int timestamp with microseconds"""
    if isinstance(data, datetime.datetime):
        if data.tzinfo is not None:
            delta = (data - epoch)
            return int(delta.total_seconds() * MCS_PER_SECOND)

        # On Windows, mktime does not support pre-epoch, see e.g.
        # https://stackoverflow.com/questions/2518706/python-mktime-overflow-error
        if is_windows:
            delta = (data - epoch_naive)
            return int(delta.total_seconds() * MCS_PER_SECOND)
        else:
            t = int(time.mktime(data.timetuple())) * MCS_PER_SECOND + \
                data.microsecond
            return t
    else:
        return data


def prepare_date(data, schema):
    """Converts datetime.date to int timestamp"""
    if isinstance(data, datetime.date):
        return data.toordinal() - DAYS_SHIFT
    elif isinstance(data, str):
        days = datetime.datetime.strptime(data, "%Y-%m-%d").toordinal()
        return days - DAYS_SHIFT
    else:
        return data


def prepare_bytes_decimal(data, schema):
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


def prepare_fixed_decimal(data, schema):
    """Converts decimal.Decimal to fixed length bytes array"""
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

    tmp = MemoryIO()

    if sign:
        unscaled_datum = (1 << bits_req) - unscaled_datum
        unscaled_datum = mask | unscaled_datum
        for index in range(size - 1, -1, -1):
            bits_to_write = unscaled_datum >> (8 * index)
            tmp.write(mk_bits(bits_to_write & 0xff))
    else:
        for i in range(offset_bits // 8):
            tmp.write(mk_bits(0))
        for index in range(bytes_req - 1, -1, -1):
            bits_to_write = unscaled_datum >> (8 * index)
            tmp.write(mk_bits(bits_to_write & 0xff))

    return tmp.getvalue()


def prepare_uuid(data, schema):
    """Converts uuid.UUID to
    string formatted UUID xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    """
    if isinstance(data, uuid.UUID):
        return str(data)
    else:
        return data


def prepare_time_millis(data, schema):
    """Convert datetime.time to int timestamp with milliseconds"""
    if isinstance(data, datetime.time):
        return int(
            data.hour * MLS_PER_HOUR + data.minute * MLS_PER_MINUTE
            + data.second * MLS_PER_SECOND + int(data.microsecond / 1000))
    else:
        return data


def prepare_time_micros(data, schema):
    """Convert datetime.time to int timestamp with microseconds"""
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
