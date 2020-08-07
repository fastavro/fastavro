from datetime import datetime, timedelta
import pytest
import time

from pytz import utc


@pytest.fixture(scope="function", autouse=True)
def look_for_cython_error(capfd):
    """If an exception happens in cython and we don't handle it properly it
    will usually print a message to stderr saying that an exception was
    ignored. Here we check for that in every test function."""
    yield
    _, err = capfd.readouterr()
    assert "Exception ignored" not in err


def assert_naive_datetime_equal_to_tz_datetime(naive_datetime, tz_datetime):
    # mktime appears to ignore microseconds, so do this manually
    microseconds = int(time.mktime(naive_datetime.timetuple())) * 1000 * 1000
    microseconds += naive_datetime.microsecond
    aware_datetime = (
        datetime(1970, 1, 1, tzinfo=utc)
        + timedelta(microseconds=microseconds)
    )
    assert aware_datetime == tz_datetime
