"""Database operation with sample table."""

from __future__ import annotations

import datetime
import random
import string
from typing import get_args

from simple_sqlite3_orm import ORMBase
from tests.sample_db.table import (
    SampleTable,
    Choice123,
    ChoiceABC,
    Mystr,
    SomeIntLiteral,
    SomeStrLiteral,
)

# for reprducible test.
random.seed(0)


class SampleDB(ORMBase[SampleTable]):
    """ORM for SampleTable."""


def _generate_random_str(_len: int = 32) -> str:
    """Generate a random string with <_len> characters.
    NOTE: this is NOT safe for generating a password!
    """
    return "".join(random.choice(string.printable) for _ in range(_len))


def generate_testdata(num_of_entry: int) -> dict[str, SampleTable]:
    _res: dict[str, SampleTable] = {}
    for idx in range(num_of_entry):
        _prim_key = Mystr(_generate_random_str())
        _now = datetime.datetime.utcnow()

        _res[_prim_key] = SampleTable(
            datetime_iso8601=_now,
            datetime_unix_float=_now,
            datetime_unix_sec=_now,
            choice_abc=random.choice(list(ChoiceABC)),
            optional_choice_123=random.choice(list(Choice123) + [None]),
            optional_num_literal=random.choice(list(get_args(SomeIntLiteral)) + [None]),
            str_literal=random.choice(get_args(SomeStrLiteral)),
            key_id=idx,
            prim_key=_prim_key,
            prim_key_hash=_prim_key.hash,
            prim_key_bln=_prim_key.bool,
            prim_key_magicf=_prim_key.magicf,
        )
    return _res
