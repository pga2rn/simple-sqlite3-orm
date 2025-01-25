"""Types helper definition."""

from __future__ import annotations

import datetime
from typing import Any

from pydantic import BeforeValidator, PlainSerializer
from typing_extensions import Annotated

#
# ------ datetime related ------ #
#


def _datetime_validator(
    _in: int | float | str | datetime.datetime | Any,
) -> datetime.datetime:
    """A pydantic validator helper for parsing serialized datetime from database."""
    if isinstance(_in, datetime.datetime):
        return _in
    elif isinstance(_in, (int, float)):
        return datetime.datetime.fromtimestamp(_in)
    elif isinstance(_in, str):
        return datetime.datetime.fromisoformat(_in)
    else:
        raise ValueError(f"{_in=} is not a valid datetime")


# NOTE(20240402): pydantic's datetime parsing will add tzinfo into the
#   validated result. This behavior is different from datetime.fromtimestamp.
#   we don't want this behavior, so we use datetime.fromtimestamp directly.
DatetimeUnixTimestamp = Annotated[
    datetime.datetime,
    BeforeValidator(_datetime_validator),
    PlainSerializer(lambda x: x.timestamp(), return_type=float),
]
"""datetime.datetime as unixtimestamp in seconds serialized as a float in db."""

DatetimeUnixTimestampInt = Annotated[
    datetime.datetime,
    BeforeValidator(_datetime_validator),
    PlainSerializer(lambda x: int(x.timestamp()), return_type=int),
]
"""datetime.datetime as unixtimestamp in seconds serialized as an int in db."""

DatetimeISO8601 = Annotated[
    datetime.datetime,
    BeforeValidator(_datetime_validator),
    PlainSerializer(lambda x: x.isoformat(), return_type=str),
]
"""datetime.datetime as str(ISO8601 formatted) serialized as a string in db."""
