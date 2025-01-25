from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from typing import Any, Iterable, Optional

import pytest
from typing_extensions import Annotated

from simple_sqlite3_orm import ConstrainRepr, TableSpec


class SimpleTableForTest(TableSpec):
    id: Annotated[
        int,
        ConstrainRepr("PRIMARY KEY"),
    ]

    id_str: Annotated[
        str,
        ConstrainRepr("NOT NULL"),
    ]

    extra: Optional[float] = None


TBL_NAME = "test_table"


class TestTableSpecWithDB:
    """A quick and simple test to test through normal usage of tablespec"""

    ENTRY_FOR_TEST = SimpleTableForTest(id=123, id_str="123", extra=0.123)

    @pytest.fixture(scope="class")
    def db_conn(self):
        conn = sqlite3.connect(":memory:")
        try:
            yield conn
        finally:
            conn.close()

    def test_table_create(self, db_conn: sqlite3.Connection):
        table_create_stmt = SimpleTableForTest.table_create_stmt(table_name=TBL_NAME)
        with db_conn as _conn:
            _conn.execute(table_create_stmt)

    def test_insert_entry(self, db_conn: sqlite3.Connection):
        _to_insert = self.ENTRY_FOR_TEST
        table_insert_stmt = SimpleTableForTest.table_insert_stmt(insert_into=TBL_NAME)
        with db_conn as _conn:
            _conn.execute(table_insert_stmt, _to_insert.table_dump_asdict())

    def test_lookup_entry(self, db_conn: sqlite3.Connection):
        _to_lookup = self.ENTRY_FOR_TEST
        table_select_stmt = SimpleTableForTest.table_select_stmt(
            select_from=TBL_NAME, select_cols="rowid, *", where_cols=("id",)
        )
        with db_conn as _conn:
            _cur = _conn.execute(table_select_stmt, {"id": _to_lookup.id})
            _cur.row_factory = SimpleTableForTest.table_row_factory2

            res = _cur.fetchall()
            assert len(res) == 1
            assert isinstance(res[0], SimpleTableForTest)
            assert res[0] == _to_lookup


@pytest.mark.parametrize(
    "_in, _validate, _expected",
    (
        ([1, "1", 1.0], True, SimpleTableForTest(id=1, id_str="1", extra=1.0)),
        ([1, "1", 1.0], False, SimpleTableForTest(id=1, id_str="1", extra=1.0)),
    ),
)
def test_table_from_tuple(
    _in: Iterable[Any], _validate: bool, _expected: SimpleTableForTest
):
    assert (
        SimpleTableForTest.table_from_tuple(_in, with_validation=_validate) == _expected
    )


@pytest.mark.parametrize(
    "_in, _validate, _expected",
    (
        (
            {"id": 1, "id_str": "1", "extra": 1.0},
            True,
            SimpleTableForTest(id=1, id_str="1", extra=1.0),
        ),
        (
            {"id": 1, "id_str": "1", "extra": 1.0},
            False,
            SimpleTableForTest(id=1, id_str="1", extra=1.0),
        ),
    ),
)
def test_table_from_dict(
    _in: Mapping[str, Any], _validate: bool, _expected: SimpleTableForTest
):
    assert (
        SimpleTableForTest.table_from_dict(_in, with_validation=_validate) == _expected
    )


@pytest.mark.parametrize(
    "_in, _expected",
    (
        (
            SimpleTableForTest(id=1, id_str="1", extra=1.0),
            {"id": 1, "id_str": "1", "extra": 1.0},
        ),
        (
            SimpleTableForTest(id=1, id_str="1", extra=1.0),
            {"id": 1, "extra": 1.0},
        ),
    ),
)
def test_table_dump_asdict(_in: SimpleTableForTest, _expected: dict[str, Any]):
    assert _in.table_dump_asdict(*_expected) == _expected


@pytest.mark.parametrize(
    "_in, _cols, _expected",
    (
        (
            SimpleTableForTest(id=1, id_str="1", extra=1.0),
            ["id", "id_str", "extra"],
            (1, "1", 1.0),
        ),
        (
            SimpleTableForTest(id=1, id_str="1", extra=1.0),
            ["extra"],
            (1.0,),
        ),
    ),
)
def test_table_dump_astuple(
    _in: SimpleTableForTest, _cols: tuple[str, ...], _expected: tuple[Any, ...]
):
    assert _in.table_dump_astuple(*_cols) == _expected
