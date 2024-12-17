from __future__ import annotations

import datetime
import random
import sqlite3
import string
import time
from typing import Callable, Generator, get_args

import pytest

from simple_sqlite3_orm import utils
from tests.sample_db.orm import SampleDB
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

# test config

TEST_ENTRY_NUM = 120_000
SELECT_ALL_BATCH_SIZE = 200
TEST_REMOVE_ENTRIES_NUM = 500
TEST_LOOKUP_ENTRIES_NUM = 5000
TEST_INSERT_BATCH_SIZE = 256
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


@pytest.fixture(scope="class")
def setup_test_db(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[SampleDB, None, None]:
    """Setup a single db connection for a test class."""

    def _con_factory():
        tmp_path = tmp_path_factory.mktemp("tmp_db_path")
        db_file = tmp_path / "test_db_file.sqlite3"

        con = sqlite3.connect(db_file)

        # enable optimization
        utils.enable_wal_mode(con, relax_sync_mode=True)
        utils.enable_mmap(con)
        utils.enable_tmp_store_at_memory(con)
        return con

    yield (orm := SampleDB(_con_factory, table_name=TABLE_NAME))

    # finally, do a database integrity check after test operations
    assert utils.check_db_integrity(orm.orm_con)


DB_LOCK_WAIT_TIMEOUT = 30


@pytest.fixture(scope="class")
def setup_con_factory(
    tmp_path_factory: pytest.TempPathFactory,
) -> Callable[[type[sqlite3.Connection] | None], sqlite3.Connection]:
    tmp_path = tmp_path_factory.mktemp("tmp_db_path")
    db_file = tmp_path / "test_db_file.sqlite3"

    def con_factory(_con_factory: type[sqlite3.Connection] | None = None):
        if _con_factory is None:
            _con_factory = sqlite3.Connection

        con = sqlite3.connect(
            db_file,
            check_same_thread=False,
            timeout=DB_LOCK_WAIT_TIMEOUT,
            factory=_con_factory,
        )
        # enable optimization
        utils.enable_wal_mode(con, relax_sync_mode=True)
        utils.enable_mmap(con)
        utils.enable_tmp_store_at_memory(con)
        return con

    return con_factory
