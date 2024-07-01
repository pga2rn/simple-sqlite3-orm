from __future__ import annotations

import datetime
import random
import string
import time
from typing import get_args

import pytest

from tests.sample_db.table import (
    Choice123,
    ChoiceABC,
    Mystr,
    SampleTable,
    SomeIntLiteral,
    SomeStrLiteral,
)

# for reproducible test
random.seed(0)


def _generate_random_str(_len: int = 128) -> str:
    """Generate a random string with <_len> characters.
    NOTE: this is NOT safe for generating a password!
    """
    return "".join(random.choice(string.printable) for _ in range(_len))


@pytest.fixture(scope="session")
def generate_testdata(num_of_entry: int) -> dict[str, SampleTable]:
    res: dict[str, SampleTable] = {}
    for idx in range(num_of_entry):
        while True:  # generate a unique prim_key
            prim_key = Mystr(_generate_random_str())
            if prim_key not in res:
                break

        now = datetime.datetime.fromtimestamp(time.time())

        res[prim_key] = SampleTable(
            unix_timestamp=now,
            unix_timestamp_int=now,
            datetime_iso8601=now,
            choice_abc=random.choice(list(ChoiceABC)),
            optional_choice_123=random.choice(list(Choice123) + [None]),
            optional_num_literal=random.choice(list(get_args(SomeIntLiteral)) + [None]),
            str_literal=random.choice(get_args(SomeStrLiteral)),
            key_id=idx,
            prim_key=prim_key,
            prim_key_sha256hash=prim_key.sha256hash,
            prim_key_bln=prim_key.bool,
            prim_key_magicf=prim_key.magicf,
        )
    return res
