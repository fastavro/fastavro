"""Implementation of abstract base class tzinfo"""
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import time
from datetime import datetime
from datetime import timedelta
from datetime import tzinfo

"""Inspired by https://github.com/apache/avro/pull/207
Obsoleted by datetime.timezone in py3.2"""


class UTCTzinfo(tzinfo):

    def utcoffset(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return timedelta(0)


utc = UTCTzinfo()

# used to compute timestamp for tz-aware datetime objects
epoch = datetime(1970, 1, 1, tzinfo=utc)


def assert_naive_datetime_equal_to_tz_datetime(naive_datetime, tz_datetime):
    # mktime appears to ignore microseconds, so do this manually
    timestamp = int(time.mktime(naive_datetime.timetuple()))
    timestamp += float(naive_datetime.microsecond) / 1000 / 1000
    aware_datetime = datetime.fromtimestamp(timestamp, tz=utc)
    assert aware_datetime == tz_datetime
