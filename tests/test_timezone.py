import datetime
from io import BytesIO

import fastavro
from fastavro.const import MCS_PER_SECOND, MLS_PER_SECOND
from fastavro._logical_writers_py import prepare_timestamp_micros
from fastavro._logical_writers_py import prepare_timestamp_millis
import pytest
from .conftest import assert_naive_datetime_equal_to_tz_datetime
from datetime import timezone


schema = {
    "fields": [
        {
            "name": "timestamp-millis",
            "type": {"type": "long", "logicalType": "timestamp-millis"},
        },
        {
            "name": "timestamp-micros",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
        },
    ],
    "namespace": "namespace",
    "name": "name",
    "type": "record",
}


# Test Time Zone with fixed offset and no DST
class TSTTzinfo(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours=10)

    def tzname(self, dt):
        return "TST"

    def dst(self, dt):
        return datetime.timedelta(0)


tst = TSTTzinfo()


def serialize(schema, data):
    bytes_writer = BytesIO()
    fastavro.schemaless_writer(bytes_writer, schema, data)
    return bytes_writer.getvalue()


def deserialize(schema, binary):
    bytes_writer = BytesIO()
    bytes_writer.write(binary)
    bytes_writer.seek(0)

    res = fastavro.schemaless_reader(bytes_writer, schema)
    return res


def timestamp_mls_from_timestamp(timestamp):
    # need to remove precision from microseconds field
    mls = int(timestamp.microsecond / 1000) * 1000
    return datetime.datetime(
        year=timestamp.year,
        month=timestamp.month,
        day=timestamp.day,
        hour=timestamp.hour,
        minute=timestamp.minute,
        second=timestamp.second,
        microsecond=mls,
        tzinfo=timestamp.tzinfo,
    )


def test_tz_attributes():
    assert tst.tzname(None) == "TST"
    assert tst.utcoffset(None) != timezone.utc.utcoffset(None)


@pytest.fixture(scope="session")
def timestamp_data():
    timestamp = datetime.datetime.now(tz=timezone.utc)
    return {
        "timestamp-millis": timestamp_mls_from_timestamp(timestamp),
        "timestamp-micros": timestamp,
    }


@pytest.fixture(scope="session")
def timestamp_data_naive():
    timestamp = datetime.datetime.now()
    return {
        "timestamp-millis": timestamp_mls_from_timestamp(timestamp),
        "timestamp-micros": timestamp,
    }


@pytest.fixture(scope="session")
def read_data(timestamp_data):
    binary = serialize(schema, timestamp_data)
    return deserialize(schema, binary)


@pytest.fixture(scope="session")
def read_data_naive(timestamp_data_naive):
    binary = serialize(schema, timestamp_data_naive)
    return deserialize(schema, binary)


def test_timestamp_micros_tz_input(timestamp_data, read_data):
    original = timestamp_data["timestamp-micros"]
    assert original.tzinfo is not None
    read = read_data["timestamp-micros"]
    assert read.tzinfo is not None
    assert original == read
    read_in_test_tz = read.astimezone(tst)
    assert original == read_in_test_tz


def test_timestamp_millis_tz_input(timestamp_data, read_data):
    original = timestamp_data["timestamp-millis"]
    assert original.tzinfo is not None
    read = read_data["timestamp-millis"]
    assert read.tzinfo is not None
    assert original == read
    read_in_test_tz = read.astimezone(tst)
    assert original == read_in_test_tz


def test_timestamp_micros_naive_input(timestamp_data_naive, read_data_naive):
    original = timestamp_data_naive["timestamp-micros"]
    assert original.tzinfo is None
    read = read_data_naive["timestamp-micros"]
    assert read.tzinfo is not None
    assert_naive_datetime_equal_to_tz_datetime(original, read)


def test_timestamp_millis_naive_input(timestamp_data_naive, read_data_naive):
    original = timestamp_data_naive["timestamp-millis"]
    assert original.tzinfo is None
    read = read_data_naive["timestamp-millis"]
    assert read.tzinfo is not None
    assert_naive_datetime_equal_to_tz_datetime(original, read)


def test_prepare_timestamp_micros():
    # seconds from epoch == 1234567890
    reference_time = datetime.datetime(2009, 2, 13, 23, 31, 30, tzinfo=timezone.utc)
    mcs_from_epoch = 1234567890 * MCS_PER_SECOND
    assert prepare_timestamp_micros(reference_time, schema) == mcs_from_epoch
    timestamp_tst = reference_time.astimezone(tst)
    assert prepare_timestamp_micros(reference_time, schema) == prepare_timestamp_micros(
        timestamp_tst, schema
    )


def test_prepare_timestamp_millis():
    # seconds from epoch == 1234567890
    reference_time = datetime.datetime(2009, 2, 13, 23, 31, 30, tzinfo=timezone.utc)
    mcs_from_epoch = 1234567890 * MLS_PER_SECOND
    assert prepare_timestamp_millis(reference_time, schema) == mcs_from_epoch
    timestamp_tst = reference_time.astimezone(tst)
    assert prepare_timestamp_millis(reference_time, schema) == prepare_timestamp_millis(
        timestamp_tst, schema
    )


@pytest.mark.parametrize(
    "my_date",
    [
        datetime.datetime(1974, 4, 4, 0, 0, 0, 1000, tzinfo=timezone.utc),
        datetime.datetime(2515, 1, 1, 0, 0, 0, 37000, tzinfo=timezone.utc),
        datetime.datetime(881, 1, 1, 0, 0, 0, 257000, tzinfo=timezone.utc),
        datetime.datetime(2243, 1, 1, 0, 0, 0, 64000, tzinfo=timezone.utc),
    ],
)
def test_problematic_timestamp_millis(my_date):
    schema = {
        "fields": [
            {
                "name": "timestamp-millis",
                "type": {"type": "long", "logicalType": "timestamp-millis"},
            },
            {
                "name": "timestamp-micros",
                "type": {"type": "long", "logicalType": "timestamp-micros"},
            },
        ],
        "name": "test_problematic_timestamp_millis",
        "type": "record",
    }

    binary = serialize(
        schema, {"timestamp-millis": my_date, "timestamp-micros": my_date}
    )
    roundtrip_data = deserialize(schema, binary)

    assert my_date == roundtrip_data["timestamp-millis"]
    assert my_date == roundtrip_data["timestamp-micros"]


@pytest.mark.parametrize(
    "my_date",
    [
        datetime.datetime(1974, 4, 4, 0, 0, 0, 1000),
        datetime.datetime(2515, 1, 1, 0, 0, 0, 37000),
        datetime.datetime(2243, 1, 1, 0, 0, 0, 64000),
    ],
)
def test_problematic_timestamp_millis_naive_time(my_date):
    schema = {
        "fields": [
            {
                "name": "timestamp-millis",
                "type": {"type": "long", "logicalType": "timestamp-millis"},
            },
            {
                "name": "timestamp-micros",
                "type": {"type": "long", "logicalType": "timestamp-micros"},
            },
        ],
        "name": "test_problematic_timestamp_millis",
        "type": "record",
    }

    binary = serialize(
        schema, {"timestamp-millis": my_date, "timestamp-micros": my_date}
    )
    roundtrip_data = deserialize(schema, binary)

    assert_naive_datetime_equal_to_tz_datetime(
        my_date, roundtrip_data["timestamp-millis"]
    )
    assert_naive_datetime_equal_to_tz_datetime(
        my_date, roundtrip_data["timestamp-micros"]
    )
