from __future__ import annotations

import logging
import sqlite3
import sys

import pytest

from simple_sqlite3_orm._utils import gen_sql_stmt
from simple_sqlite3_orm.utils import (
    batched,
    check_db_integrity,
    check_pragma_compile_time_options,
    concatenate_condition,
    enable_mmap,
    enable_tmp_store_at_memory,
    enable_wal_mode,
    gen_check_constrain,
    lookup_table,
    optimize_db,
    wrap_value,
)
from tests.sample_db._types import Choice123, ChoiceABC, SomeIntLiteral, SomeStrLiteral

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "func, args, kwargs",
    (
        (enable_wal_mode, (), {}),
        (enable_wal_mode, (False,), {}),
        (enable_tmp_store_at_memory, (), {}),
        (enable_mmap, (32 * 1024**2,), {}),
        (enable_mmap, (), {}),
        (optimize_db, (), {}),
    ),
)
def test_pragma_enable_helpers(func, args, kwargs):
    with sqlite3.connect(":memory:") as conn:
        func(conn, *args, **kwargs)


def test_check_db_integrity():
    with sqlite3.connect(":memory:") as conn:
        with conn:
            conn.execute(
                "CREATE TABLE test_table (key TEXT PRIMARY KEY) WITHOUT ROWID;"
            )
            conn.execute("INSERT INTO test_table (key) VALUES (?)", ("aaabbbccc",))

        check_db_integrity(conn)
        check_db_integrity(conn, table_name="test_table")


def test_lookup_table():
    with sqlite3.connect(":memory:") as conn:
        table_name = "test_table"
        with conn:
            conn.execute(
                f"CREATE TABLE {table_name} (key TEXT PRIMARY KEY) WITHOUT ROWID;"
            )

        assert lookup_table(conn, table_name)


def test_checkpragma_compile_time_options():
    with sqlite3.connect(":memory:") as conn:
        logger.info(f"runtime sqlite3 library version: {sqlite3.sqlite_version}")

        compile_time_options = check_pragma_compile_time_options(conn)
        logger.info(f"all options: {compile_time_options}")
        assert compile_time_options

        threadsafe_level = check_pragma_compile_time_options(conn, "THREADSAFE")
        logger.info(f"THREADSAFE: {threadsafe_level}")
        assert threadsafe_level is not None


if sys.version_info < (3, 12):

    @pytest.mark.parametrize(
        "iterable, n, expected",
        (
            (
                range(1, 14),
                3,
                [(1, 2, 3), (4, 5, 6), (7, 8, 9), (10, 11, 12), (13,)],
            ),
            (
                range(1, 7),
                2,
                [(1, 2), (3, 4), (5, 6)],
            ),
        ),
    )
    def test_batched(iterable, n, expected):
        for i, b in enumerate(batched(iterable, n)):
            assert expected[i] == b


FIELD_NAME = "test_field"


@pytest.mark.parametrize(
    "_in, expected",
    (
        (Choice123, f"{FIELD_NAME} IN (1,2,3)"),
        (ChoiceABC, rf'{FIELD_NAME} IN ("A","B","C")'),
        (SomeIntLiteral, f"{FIELD_NAME} IN (123,456,789)"),
        (SomeStrLiteral, rf'{FIELD_NAME} IN ("H","I","J")'),
        (object(), TypeError()),
        (type(None), TypeError()),
    ),
)
def test_gen_check_constrain(_in, expected):
    if isinstance(expected, Exception):
        with pytest.raises(type(expected)):
            gen_check_constrain(_in, FIELD_NAME)
    else:
        assert gen_check_constrain(_in, FIELD_NAME) == expected


@pytest.mark.parametrize(
    "stmts, with_parenthese, expected",
    (
        (
            ["column", "IS NULL", "OR", "column IN (1,2,3)"],
            True,
            "(column IS NULL OR column IN (1,2,3))",
        ),
        (
            ["column", "IS NULL", "OR", "column IN (1,2,3)"],
            False,
            "column IS NULL OR column IN (1,2,3)",
        ),
    ),
)
def test_concatenate_condition(stmts, with_parenthese, expected):
    assert (
        concatenate_condition(*stmts, wrapped_with_parentheses=with_parenthese)
        == expected
    )


@pytest.mark.parametrize(
    "value, expected",
    (
        (123, r"123"),
        (123.456, r"123.456"),
        ("a_string", r'"a_string"'),
        (None, r"NULL"),
        (bytes.fromhex("1234567890AABBCC"), r"x'1234567890aabbcc'"),
        (Choice123.ONE, r"1"),
        (ChoiceABC.A, r'"A"'),
    ),
)
def test_wrap_value(value, expected):
    assert wrap_value(value) == expected


_not_set = object()


@pytest.mark.parametrize(
    "components, end_with, expected",
    (
        (
            [
                "        WHERE",
                "id",
                "=",
                ":check_id",
                "AND",
                "",
                "type",
                "<>",
                "'A'",
            ],
            "\n",
            # NOTE: \n is stripped
            "WHERE id = :check_id AND type <> 'A'",
        ),
        (
            ["    SELECT", "count(*)", "FROM", "some_table"],
            _not_set,
            "SELECT count(*) FROM some_table;",
        ),
    ),
)
def test_gen_sql_stmt(components, end_with, expected):
    if end_with is _not_set:
        assert gen_sql_stmt(*components) == expected
    else:
        assert gen_sql_stmt(*components, end_with=end_with) == expected
