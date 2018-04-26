"""Timezone class for dealing with timezone-aware datetime objects
Inspired by https://github.com/apache/avro/pull/207"""
from datetime import datetime
from datetime import timedelta
from datetime import tzinfo


class UTCTzinfo(tzinfo):
    """Implementation of abstract base class tzinfo,
    python >= 3.2 can use datetime.timezone.utc instead"""
    def utcoffset(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return timedelta(0)


utc = UTCTzinfo()

# used to compute timestamp for tz-aware datetime objects
# python >= 3.3 can use datetime.datetime.timestamp() instead
epoch = datetime(1970, 1, 1, tzinfo=utc)
