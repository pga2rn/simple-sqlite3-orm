from __future__ import annotations

import contextlib
import random
import sqlite3
from pathlib import Path
from typing import Callable, Generator, Literal, Optional, TypedDict

import pytest
from pydantic import PlainSerializer, PlainValidator
from typing_extensions import Annotated

from simple_sqlite3_orm import (
    ColsSelectFactory,
    ConstrainRepr,
    TableSpec,
    TypeAffinityRepr,
    utils,
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


SimpleTableForTestColsSelect = ColsSelectFactory[
    Literal["id", "id_str", "extra", "int_str"]
]

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
            timeout=DB_LOCK_WAIT_TIMEOUT,
            factory=_con_factory,
        )
        # enable optimization
        utils.enable_wal_mode(con, relax_sync_mode=True)
        utils.enable_mmap(con)
        utils.enable_tmp_store_at_memory(con)
        return con

    return con_factory
