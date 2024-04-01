"""Types helper definition."""

from __future__ import annotations

import datetime

from pydantic import PlainSerializer
from typing_extensions import Annotated


#
# ------ datetime related ------ #
#

DatetimeUnix = Annotated[
    datetime.datetime,
    PlainSerializer(lambda x: int(x.timestamp()), return_type=int),
]
"""datetime.datetime serialized as int(unixtimestamp in seconds) in db."""

DatetimeUnixNS = Annotated[
    datetime.datetime,
    PlainSerializer(lambda x: x.timestamp(), return_type=float),
]
"""datetime.datetime serialized as float(unixtimestamp in nanoseconds) in db."""

DatetimeISO8601 = Annotated[
    datetime.datetime,
    PlainSerializer(lambda x: x.isoformat(), return_type=str),
]
"""datetime.datetime serialized as str(ISO8601 formatted) in db."""
