import pytest
import os
import random
import time
from datetime import timezone, datetime, timedelta

from fastavro.read import _read as _reader
from fastavro.write import _write as _writer

SEED = time.time()
random.seed(SEED)
is_windows = os.name == "nt"
epoch_naive = datetime(1970, 1, 1)


def pytest_report_header(config):
    return f"SEED is {SEED}"


@pytest.fixture(scope="function", autouse=True)
def look_for_cython_error(capfd):
    """If an exception happens in cython and we don't handle it properly it
    will usually print a message to stderr saying that an exception was
    ignored. Here we check for that in every test function."""
    yield
    _, err = capfd.readouterr()
    assert "Exception ignored" not in err


def assert_naive_datetime_equal_to_tz_datetime(naive_datetime, tz_datetime):
    # On Windows, mktime does not support pre-epoch, see e.g.
    # https://stackoverflow.com/questions/2518706/python-mktime-overflow-error
    if is_windows:
        delta = naive_datetime - epoch_naive
        microseconds = (
            delta.days * 24 * 3600 + delta.seconds
        ) * 1000 * 1000 + delta.microseconds
    else:
        # mktime appears to ignore microseconds, so do this manually
        microseconds = int(time.mktime(naive_datetime.timetuple())) * 1000 * 1000
        microseconds += naive_datetime.microsecond

    aware_datetime = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
        microseconds=microseconds
    )
    assert aware_datetime == tz_datetime


def is_testing_cython_modules() -> bool:
    return hasattr(_reader, "CYTHON_MODULE") and hasattr(_writer, "CYTHON_MODULE")


def is_testing_pure_python() -> bool:
    # Use this rather than hasattr(sys, "pypy_version_info") for times when we want to be able to test locally
    # with pure python
    return not is_testing_cython_modules()
