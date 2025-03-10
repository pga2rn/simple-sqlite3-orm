from __future__ import annotations

import contextlib
import datetime
import random
import sqlite3
import string
import time
from pathlib import Path
from typing import Callable, Generator, Optional, TypedDict, get_args

import pytest
from pydantic import PlainSerializer, PlainValidator
from typing_extensions import Annotated

from simple_sqlite3_orm import ConstrainRepr, TableSpec, TypeAffinityRepr, utils
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

# simple table for test

ID_STR_DEFAULT_VALUE = "9800"


class SimpleTableForTest(TableSpec):
    id: Annotated[
        int,
        ConstrainRepr("PRIMARY KEY"),
    ]

    id_str: Annotated[
        str,
        ConstrainRepr("NOT NULL", ("DEFAULT", utils.wrap_value(ID_STR_DEFAULT_VALUE))),
    ]

    extra: Optional[float] = None
    int_str: Annotated[
        int,
        TypeAffinityRepr(str),
        PlainSerializer(lambda x: str(x)),
        PlainValidator(lambda x: int(x)),
    ] = 0


class SimpleTableForTestCols(TypedDict, total=False):
    id: int
    id_str: str
    extra: Optional[float]
    int_str: int


# sqlite3 lib features set


class SQLITE3_COMPILE_OPTION_FLAGS:
    with contextlib.closing(sqlite3.connect(":memory:")) as conn:
        RETURNING_AVAILABLE = sqlite3.sqlite_version_info >= (3, 35, 0)
        SQLITE_ENABLE_UPDATE_DELETE_LIMIT = bool(
            utils.check_pragma_compile_time_options(
                conn, "SQLITE_ENABLE_UPDATE_DELETE_LIMIT"
            )
        )


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
def setup_test_db_conn(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[sqlite3.Connection, None, None]:
    """Setup a single db connection for a test class."""

    tmp_path = tmp_path_factory.mktemp("tmp_db_path")
    db_file = tmp_path / "test_db_file.sqlite3"

    with contextlib.closing(sqlite3.connect(db_file)) as conn:
        # enable optimization
        utils.enable_wal_mode(conn, relax_sync_mode=True)
        utils.enable_mmap(conn)
        utils.enable_tmp_store_at_memory(conn)

        yield conn
        # finally, do a database integrity check after test operations
        assert utils.check_db_integrity(conn)


@pytest.fixture
def db_conn_func_scope(
    tmp_path: Path,
) -> Generator[sqlite3.Connection]:
    """Setup a single db connection for a test class."""
    db_file = tmp_path / "test_db_file.sqlite3"

    with contextlib.closing(sqlite3.connect(db_file)) as conn:
        # enable optimization
        utils.enable_wal_mode(conn, relax_sync_mode=True)
        utils.enable_mmap(conn)
        utils.enable_tmp_store_at_memory(conn)

        yield conn


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
