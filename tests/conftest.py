import datetime
import pytest
import time

from fastavro.read import READERS
from fastavro.write import WRITERS
from fastavro._schema_common import SCHEMA_DEFS
from fastavro._timezone import utc


@pytest.fixture(scope='function')
def clean_readers_writers_and_schemas():
    reader_keys = {key for key in READERS.keys()}
    writer_keys = {key for key in WRITERS.keys()}
    schema_keys = {key for key in SCHEMA_DEFS.keys()}

    yield

    repo_keys = (
        (READERS, reader_keys),
        (WRITERS, writer_keys),
        (SCHEMA_DEFS, schema_keys),
    )
    for repo, keys in repo_keys:
        diff = set(repo) - keys
        for key in diff:
            del repo[key]


def assert_naive_datetime_equal_to_tz_datetime(naive_datetime, tz_datetime):
    # mktime appears to ignore microseconds, so do this manually
    timestamp = int(time.mktime(naive_datetime.timetuple()))
    timestamp += float(naive_datetime.microsecond) / 1000 / 1000
    aware_datetime = datetime.datetime.fromtimestamp(timestamp, tz=utc)
    assert aware_datetime == tz_datetime
