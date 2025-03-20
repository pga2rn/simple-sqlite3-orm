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

TEST_ENTRY_NUM = 120_000
TEST_REMOVE_ENTRIES_NUM = 500
TEST_LOOKUP_ENTRIES_NUM = 5000
TEST_INSERT_BATCH_SIZE = 3000
PRIM_KEY_LEN = 128
TABLE_NAME = "test_table"
INDEX_NAME = "key_id_prim_key_hash_idx"
INDEX_KEYS = ("key_id", "prim_key_sha256hash")


def _generate_random_str(_len: int = PRIM_KEY_LEN) -> str:
    """Generate a random string with <_len> characters.
    NOTE: this is NOT safe for generating a password!
    """
    return "".join(random.choice(string.printable) for _ in range(_len))


def generate_test_data(num_of_entry: int) -> dict[str, SampleTable]:
    res: dict[str, SampleTable] = {}
    for idx in range(num_of_entry):
        while True:  # generate a unique prim_key
            prim_key = Mystr(_generate_random_str())
            if prim_key not in res:
                break

        now_timestamp = time.time()
        res[prim_key] = SampleTable(
            unix_timestamp=datetime.datetime.fromtimestamp(now_timestamp),
            unix_timestamp_int=datetime.datetime.fromtimestamp(int(now_timestamp)),
            datetime_iso8601=datetime.datetime.fromtimestamp(now_timestamp),
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


@pytest.fixture(scope="session")
def setup_test_data():
    return generate_test_data(TEST_ENTRY_NUM)


@pytest.fixture(scope="session")
def entries_to_lookup(setup_test_data: dict[str, SampleTable]) -> list[SampleTable]:
    return random.sample(
        list(setup_test_data.values()),
        k=TEST_LOOKUP_ENTRIES_NUM,
    )


@pytest.fixture(scope="session")
def entries_to_remove(setup_test_data: dict[str, SampleTable]) -> list[SampleTable]:
    return random.sample(
        list(setup_test_data.values()),
        k=TEST_REMOVE_ENTRIES_NUM,
    )


@pytest.fixture(scope="session")
def entry_to_lookup(setup_test_data: dict[str, SampleTable]) -> SampleTable:
    return random.choice(list(setup_test_data.values()))
