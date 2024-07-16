from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import BaseModel

from simple_sqlite3_orm import (
    DatetimeISO8601,
    DatetimeUnixTimestamp,
    DatetimeUnixTimestampInt,
)


class DatetimeTestModel(BaseModel):
    unix_timestamp: DatetimeUnixTimestamp
    unix_timestamp_int: DatetimeUnixTimestampInt
    datetime_iso8601: DatetimeISO8601


# NOTE: on windows platform, the timestamp must be larger than 86400,
#   otherwise datetime.timestamp method will raise OSError: Invalid argument.


@pytest.mark.parametrize(
    "_in, expected",
    (
        (
            t := datetime.fromtimestamp(ts := 1251763199.012345),
            {
                "unix_timestamp": ts,
                "unix_timestamp_int": int(ts),
                "datetime_iso8601": t.isoformat(),
            },
        ),
    ),
)
def test_datetime_helper(_in, expected):
    inst = DatetimeTestModel(
        unix_timestamp=_in,
        unix_timestamp_int=_in,
        datetime_iso8601=_in,
    )
    assert inst.model_dump() == expected
    assert DatetimeTestModel.model_validate(
        inst.model_dump()
    ) == DatetimeTestModel.model_validate(expected)
