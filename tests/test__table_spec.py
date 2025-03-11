from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Mapping
from typing import Any

import pytest

from simple_sqlite3_orm import CreateTableParams
from tests.conftest import (
    ID_STR_DEFAULT_VALUE,
    SQLITE3_COMPILE_OPTION_FLAGS,
    SimpleTableForTest,
    SimpleTableForTestCols,
)

TBL_NAME = "test_table"
ENTRY_FOR_TEST = SimpleTableForTest(id=123, id_str="123", extra=0.123, int_str=987)


@pytest.mark.parametrize(
    "table_create_params",
    (
        (CreateTableParams(if_not_exists=True)),
        (CreateTableParams(strict=True)),
        (CreateTableParams(temporary=True)),
        (CreateTableParams(without_rowid=True)),
        (
            CreateTableParams(
                if_not_exists=True, strict=True, temporary=True, without_rowid=True
            )
        ),
    ),
)
def test_table_create(table_create_params: CreateTableParams) -> None:
    with contextlib.closing(sqlite3.connect(":memory:")) as db_conn:
        table_create_stmt = SimpleTableForTest.table_create_stmt(
            table_name=TBL_NAME, **table_create_params
        )
        with db_conn as _conn:
            _conn.execute(table_create_stmt)


class TestTableSpecWithDB:
    """A quick and simple test to test through normal usage of tablespec.

    NOTE that this test is mostly focusing on syntax, i.e., to ensure that the generated
        sqlite3 query can be parsed and accepted by sqlite3 DB engine.
    """

    ENTRY_FOR_TEST = ENTRY_FOR_TEST

    @pytest.fixture
    def db_conn(self):
        with contextlib.closing(sqlite3.connect(":memory:")) as db_conn:
            table_create_stmt = SimpleTableForTest.table_create_stmt(
                table_name=TBL_NAME
            )
            with db_conn as _conn:
                _conn.execute(table_create_stmt)
            yield db_conn

    @pytest.fixture
    def prepare_test_entry(self, db_conn: sqlite3.Connection):
        _to_insert = self.ENTRY_FOR_TEST
        table_insert_stmt = SimpleTableForTest.table_insert_stmt(insert_into=TBL_NAME)
        with db_conn as _conn:
            _conn.execute(table_insert_stmt, _to_insert.table_dump_asdict())

    @pytest.mark.parametrize(
        "case, to_insert",
        (
            (
                "insert a complete row",
                (SimpleTableForTestCols(int_str=987, id=1, id_str="1", extra=0.123)),
            ),
            (
                "insert a partially set row, omit id(rowid alias)",
                (SimpleTableForTestCols(id_str="1", int_str=987, extra=0.123)),
            ),
            (
                "insert a row with order shuffled",
                (SimpleTableForTestCols(int_str=987, extra=0.123, id=1, id_str="1")),
            ),
        ),
    )
    def test_insert_entry(
        self, case, to_insert: SimpleTableForTestCols, db_conn: sqlite3.Connection
    ):
        table_insert_stmt = SimpleTableForTest.table_insert_stmt(
            insert_into=TBL_NAME, insert_cols=tuple(to_insert)
        )
        with db_conn as _conn:
            _conn.execute(table_insert_stmt, to_insert)

        with db_conn as _conn:
            _cur = _conn.execute(
                SimpleTableForTest.table_select_stmt(select_from=TBL_NAME)
            )
            _cur.row_factory = SimpleTableForTest.table_row_factory

            _res: SimpleTableForTest = _cur.fetchone()
            assert all(v == getattr(_res, k) for k, v in to_insert.items())

    def test_insert_default_values(self, db_conn: sqlite3.Connection):
        table_insert_stmt = SimpleTableForTest.table_insert_stmt(
            insert_into=TBL_NAME, insert_default=True
        )
        with db_conn as _conn:
            _conn.execute(table_insert_stmt)

        with db_conn as _conn:
            _cur = _conn.execute(
                SimpleTableForTest.table_select_stmt(select_from=TBL_NAME)
            )
            # for cols with no defautl value defined, NULL will be assigned
            # for rowid, it will be automatically incremented
            assert _cur.fetchone() == (1, ID_STR_DEFAULT_VALUE, None, None)

    def test_lookup_entry(self, db_conn: sqlite3.Connection, prepare_test_entry):
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

    #
    # ------ UPDATE sqlite3 stmt test ------ #
    #

    UPDATE_API_TEST_CASES = [
        (
            _set_values := SimpleTableForTestCols(id=678, id_str="1.23456"),
            _where_cols := ENTRY_FOR_TEST.table_dump_asdict(),
            SimpleTableForTest.table_update_stmt(
                update_target=TBL_NAME,
                set_cols=tuple(_set_values),
                where_cols=tuple(_where_cols),
            ),
            ENTRY_FOR_TEST.model_copy(update=_set_values),
        ),
        (
            _set_values := SimpleTableForTestCols(id=678, id_str="1.23456"),
            _where_cols := ENTRY_FOR_TEST.table_dump_asdict(),
            SimpleTableForTest.table_update_stmt(
                update_target=TBL_NAME,
                set_cols=tuple(_set_values),
                where_stmt=f"WHERE id = {ENTRY_FOR_TEST.id}",
            ),
            ENTRY_FOR_TEST.model_copy(update=_set_values),
        ),
        (
            _set_values := SimpleTableForTestCols(id=678, id_str="1.23456"),
            _where_cols := ENTRY_FOR_TEST.table_dump_asdict(),
            SimpleTableForTest.table_update_stmt(
                update_target=TBL_NAME,
                set_cols=tuple(_set_values),
            ),
            ENTRY_FOR_TEST.model_copy(update=_set_values),
        ),
    ]

    if SQLITE3_COMPILE_OPTION_FLAGS.SQLITE_ENABLE_UPDATE_DELETE_LIMIT:
        UPDATE_API_TEST_CASES.extend(
            [
                (
                    _set_values := SimpleTableForTestCols(
                        id=678, id_str="2.3456", extra=2.3456
                    ),
                    _where_cols := ENTRY_FOR_TEST.table_dump_asdict(),
                    SimpleTableForTest.table_update_stmt(
                        update_target=TBL_NAME,
                        set_cols=tuple(_set_values),
                        where_cols=tuple(_where_cols),
                        order_by=("id",),
                        limit=1,
                    ),
                    ENTRY_FOR_TEST.model_copy(update=_set_values),
                ),
                (
                    _set_values := SimpleTableForTestCols(
                        id=678, id_str="2.3456", extra=2.3456
                    ),
                    _where_cols := ENTRY_FOR_TEST.table_dump_asdict(),
                    SimpleTableForTest.table_update_stmt(
                        or_option="fail",
                        update_target=TBL_NAME,
                        set_cols=tuple(_set_values),
                        where_cols=tuple(_where_cols),
                        order_by=("id",),
                        limit=1,
                    ),
                    ENTRY_FOR_TEST.model_copy(update=_set_values),
                ),
            ]
        )

        if SQLITE3_COMPILE_OPTION_FLAGS.RETURNING_AVAILABLE:
            UPDATE_API_TEST_CASES.append(
                (
                    _set_values := SimpleTableForTestCols(
                        id=678, id_str="2.3456", extra=2.3456
                    ),
                    _where_cols := ENTRY_FOR_TEST.table_dump_asdict(),
                    SimpleTableForTest.table_update_stmt(
                        or_option="fail",
                        update_target=TBL_NAME,
                        set_cols=tuple(_set_values),
                        where_cols=tuple(_where_cols),
                        returning_cols="*",
                        order_by=("id",),
                        limit=1,
                    ),
                    ENTRY_FOR_TEST.model_copy(update=_set_values),
                )
            )

    if SQLITE3_COMPILE_OPTION_FLAGS.RETURNING_AVAILABLE:
        UPDATE_API_TEST_CASES.append(
            (
                _set_values := SimpleTableForTestCols(id_str="2.3456", extra=2.3456),
                _where_cols := ENTRY_FOR_TEST.table_dump_asdict(),
                SimpleTableForTest.table_update_stmt(
                    or_option="fail",
                    update_target=TBL_NAME,
                    set_cols=tuple(_set_values),
                    where_cols=tuple(_where_cols),
                    returning_cols="*",
                ),
                ENTRY_FOR_TEST.model_copy(update=_set_values),
            )
        )

    @pytest.mark.parametrize(
        "set_values, where_cols, update_stmt, expected_result", UPDATE_API_TEST_CASES
    )
    def test_update_entry(
        self,
        db_conn: sqlite3.Connection,
        set_values: Mapping[str, Any],
        where_cols: Mapping[str, Any],
        update_stmt: str,
        expected_result: SimpleTableForTest,
        prepare_test_entry,
    ):
        _params = SimpleTableForTestCols(
            **SimpleTableForTest.table_preprare_update_where_cols(where_cols),
            **set_values,
        )
        with db_conn as _conn:
            _conn.execute(update_stmt, _params)

        # NOTE: we only have one entry at the table
        with db_conn as _conn:
            _cur = _conn.execute(
                SimpleTableForTest.table_select_stmt(select_from=TBL_NAME)
            )
            _cur.row_factory = SimpleTableForTest.table_row_factory

            res = _cur.fetchone()
            assert res == expected_result

    # ------ end of UPDATE sqlite3 stmt test ------ #

    def test_deserialize_asdict_row_factory(
        self, db_conn: sqlite3.Connection, prepare_test_entry
    ):
        _stmt = SimpleTableForTest.table_select_stmt(
            select_from=TBL_NAME,
            select_cols="int_str AS str_int, int_str, count(*) AS count",
            where_cols=("id",),
        )
        with db_conn as conn:
            _cur = conn.execute(_stmt, SimpleTableForTestCols(id=ENTRY_FOR_TEST.id))
            _cur.row_factory = SimpleTableForTest.table_deserialize_asdict_row_factory

            _res: SimpleTableForTestCols = _cur.fetchone()
            assert _res == {
                # NOTE: if alias the selected col, this col's value will not be deserialized.
                "str_int": str(ENTRY_FOR_TEST.int_str),
                "int_str": ENTRY_FOR_TEST.int_str,
                "count": 1,
            }

    def test_deserialize_astuple_row_factory(
        self, db_conn: sqlite3.Connection, prepare_test_entry
    ):
        _stmt = SimpleTableForTest.table_select_stmt(
            select_from=TBL_NAME,
            select_cols="count(*) AS total_entry_num, int_str AS str_int, int_str",
            where_cols=("id",),
        )
        with db_conn as conn:
            _cur = conn.execute(_stmt, SimpleTableForTestCols(id=ENTRY_FOR_TEST.id))
            _cur.row_factory = SimpleTableForTest.table_deserialize_astuple_row_factory

            _res: SimpleTableForTestCols = _cur.fetchone()
            assert _res == (
                1,
                # NOTE: if alias the selected col, this col's value will not be deserialized.
                str(ENTRY_FOR_TEST.int_str),
                ENTRY_FOR_TEST.int_str,
            )


@pytest.mark.parametrize(
    "_in, _validate, _expected",
    (
        (
            (1, "1", 1.0, "789"),
            True,
            SimpleTableForTest(id=1, id_str="1", extra=1.0, int_str=789),
        ),
        (
            (1, "1", 1.0, 789),
            False,
            SimpleTableForTest(id=1, id_str="1", extra=1.0, int_str=789),
        ),
    ),
)
def test_table_from_tuple(
    _in: tuple[Any, ...], _validate: bool, _expected: SimpleTableForTest
):
    assert (
        SimpleTableForTest.table_from_tuple(_in, with_validation=_validate) == _expected
    )


@pytest.mark.parametrize(
    "_in, _validate, _expected",
    (
        (
            {"id": 1, "id_str": "1", "extra": 1.0, "int_str": "789"},
            True,
            SimpleTableForTest(id=1, id_str="1", extra=1.0, int_str=789),
        ),
        (
            {"id": 1, "id_str": "1", "extra": 1.0, "int_str": 789},
            False,
            SimpleTableForTest(id=1, id_str="1", extra=1.0, int_str=789),
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
            SimpleTableForTest(id=1, id_str="1", extra=1.0, int_str=789),
            {"id": 1, "id_str": "1", "extra": 1.0, "int_str": "789"},
        ),
        (
            SimpleTableForTest(id=1, id_str="1", extra=1.0, int_str=789),
            {"int_str": "789"},
        ),
    ),
)
def test_table_dump_asdict(_in: SimpleTableForTest, _expected: dict[str, Any]):
    assert _in.table_dump_asdict(*_expected) == _expected


@pytest.mark.parametrize(
    "_in, _cols, _expected",
    (
        (
            SimpleTableForTest(id=1, id_str="1", extra=1.0, int_str=789),
            ["id", "id_str", "extra", "int_str"],
            (1, "1", 1.0, "789"),
        ),
        (
            SimpleTableForTest(id=1, id_str="1", extra=1.0, int_str=789),
            ["int_str"],
            ("789",),
        ),
    ),
)
def test_table_dump_astuple(
    _in: SimpleTableForTest, _cols: list[str], _expected: tuple[Any, ...]
):
    assert _in.table_dump_astuple(*_cols) == _expected


@pytest.mark.parametrize(
    "_in, _expected",
    (
        (SimpleTableForTestCols(int_str=123), {"int_str": "123"}),
        (SimpleTableForTestCols(id=456, int_str=789), {"id": 456, "int_str": "789"}),
    ),
)
def test_serializing_mapping(_in: Mapping[str, Any], _expected: Mapping[str, Any]):
    assert SimpleTableForTest.table_serialize_mapping(_in) == _expected
