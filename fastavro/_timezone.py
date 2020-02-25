from datetime import datetime
from pytz import utc


# used to compute timestamp for tz-aware datetime objects
# python >= 3.3 can use datetime.datetime.timestamp() instead
epoch = datetime(1970, 1, 1, tzinfo=utc)
epoch_naive = datetime(1970, 1, 1)
