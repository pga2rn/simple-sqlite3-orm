from __future__ import annotations

import functools
import logging
import sqlite3
from typing import Any

import pytest

from simple_sqlite3_orm import (
    CreateIndexParams,
    CreateTableParams,
    ORMBase,
)
from simple_sqlite3_orm._orm._base import DO_NOT_CHANGE_ROW_FACTORY
from tests.conftest import (
    SELECT_ALL_BATCH_SIZE,
    SimpleTableForTest,
    SimpleTableForTestCols,
)

logger = logging.getLogger(__name__)


TBL_NAME = "test_table"


class SimpleTableORM(ORMBase[SimpleTableForTest]):
    orm_bootstrap_table_name = TBL_NAME


ENTRY_FOR_TEST = SimpleTableForTest(id=123, id_str="123", extra=0.123, int_str=987)


TABLE_SPEC_LOGGER = "simple_sqlite3_orm._table_spec"


@pytest.fixture(autouse=True)
def enable_debug_logging(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG, logger=TABLE_SPEC_LOGGER)
    yield
    for log in list(caplog.get_records("call")):
        logger.info(log)


class TestORMBase:
    @pytest.fixture(scope="class")
    def setup_connection(self):
        with sqlite3.connect(":memory:") as conn:
            orm_inst = SimpleTableORM(conn)
            yield orm_inst

    def test_create_without_rowid_table(self):
        """NOTE: to test select_all_with_pagination, we cannot specify without_rowid, so we
        create this test case dedicated for creating without_rowid table test."""
        with sqlite3.connect(":memory:") as conn:
            orm_inst = SimpleTableORM(conn)
            orm_inst.orm_create_table(without_rowid=True)

    def test_create_table(self, setup_connection: SimpleTableORM):
        setup_connection.orm_create_table(allow_existed=False)

        if sqlite3.sqlite_version_info < (3, 37, 0):
            logger.warning(
                "STRICT table option is only available after sqlite3 version 3.37, "
                f"get {sqlite3.sqlite_version_info}, skip testing STRICT table option."
            )
            setup_connection.orm_create_table(allow_existed=True)
        else:
            setup_connection.orm_create_table(allow_existed=True, strict=True)

        with pytest.raises(sqlite3.DatabaseError):
            setup_connection.orm_create_table(allow_existed=False)

    def test_create_index(self, setup_connection: SimpleTableORM):
        setup_connection.orm_create_index(
            index_name="idx_prim_key_sha256hash",
            index_keys=("prim_key_sha256hash",),
            allow_existed=True,
            unique=True,
        )

        with pytest.raises(sqlite3.DatabaseError):
            setup_connection.orm_create_index(
                index_name="idx_prim_key_sha256hash",
                index_keys=("prim_key_sha256hash",),
                allow_existed=False,
            )

    def test_insert_entries(self, setup_connection: SimpleTableORM):
        setup_connection.orm_insert_entries((ENTRY_FOR_TEST,))

        with pytest.raises(sqlite3.DatabaseError):
            setup_connection.orm_insert_entry(ENTRY_FOR_TEST, or_option="fail")
        setup_connection.orm_insert_entry(ENTRY_FOR_TEST, or_option="ignore")
        setup_connection.orm_insert_entry(ENTRY_FOR_TEST, or_option="replace")

    def test_orm_execute(self, setup_connection: SimpleTableORM):
        sql_stmt = setup_connection.orm_table_spec.table_select_stmt(
            select_from=setup_connection.orm_table_name,
            select_cols="*",
            function="count",
        )

        res = setup_connection.orm_execute(sql_stmt)
        assert res and res[0][0] > 0

    def test_orm_check_entry_exist(self, setup_connection: SimpleTableORM):
        assert setup_connection.orm_check_entry_exist(
            SimpleTableForTestCols(id=ENTRY_FOR_TEST.id)
        )
        assert setup_connection.orm_check_entry_exist(
            **SimpleTableForTestCols(int_str=ENTRY_FOR_TEST.int_str)
        )
        assert not setup_connection.orm_check_entry_exist(int_str="123")

    def test_select_entry(self, setup_connection: SimpleTableORM):
        _selected_row = setup_connection.orm_select_entry(
            SimpleTableForTestCols(id=ENTRY_FOR_TEST.id)
        )
        _selected_row2 = setup_connection.orm_select_entry(
            **SimpleTableForTestCols(id=ENTRY_FOR_TEST.id)
        )
        assert ENTRY_FOR_TEST == _selected_row == _selected_row2

    def test_select_entries(self, setup_connection: SimpleTableORM):
        select_result = setup_connection.orm_select_entries(
            SimpleTableForTestCols(id=ENTRY_FOR_TEST.id),
            _distinct=True,
            _order_by=(("key_id", "DESC"),),
            _limit=1,
        )
        select_result = list(select_result)

        select_result2 = setup_connection.orm_select_entries(
            **SimpleTableForTestCols(id=ENTRY_FOR_TEST.id),
            _distinct=True,
            _order_by=(("key_id", "DESC"),),
            _limit=1,
        )
        select_result2 = list(select_result2)

        assert len(select_result) == len(select_result2) == 1
        assert select_result[0] == select_result2[0] == ENTRY_FOR_TEST

    def test_select_all_entries(self, setup_connection: SimpleTableORM):
        select_result = setup_connection.orm_select_all_with_pagination(
            batch_size=SELECT_ALL_BATCH_SIZE
        )
        select_result = list(select_result)

        assert len(select_result) == 1
        assert select_result[0] == ENTRY_FOR_TEST

    def test_select_with_function_call(self, setup_connection: SimpleTableORM):
        _stmt = setup_connection.orm_table_spec.table_select_stmt(
            select_from=setup_connection.orm_bootstrap_table_name,
            select_cols="*",
            function="count",
        )

        with setup_connection.orm_con as con:
            cur = con.execute(_stmt)
            res = cur.fetchone()
            assert res[0] == 1

    def test_delete_entries(self, setup_connection: SimpleTableORM):
        assert (
            setup_connection.orm_delete_entries(
                SimpleTableForTestCols(id=ENTRY_FOR_TEST.id)
            )
            == 1
        )


@pytest.mark.parametrize(
    "table_name, create_table_params, create_indexes_params",
    (
        (
            "test_1",
            # NOTE: strict param only supported at sqlite3 >= 3.37
            CreateTableParams(
                if_not_exists=True,
                temporary=True,
                strict=True if sqlite3.sqlite_version_info >= (3, 37, 0) else False,
                without_rowid=True,
            ),
            None,
        ),
        (
            "test_2",
            CreateTableParams(without_rowid=True),
            [
                CreateIndexParams(
                    index_name="test_index",
                    index_cols=("key_id", "prim_key"),
                    if_not_exists=True,
                    unique=True,
                ),
                CreateIndexParams(
                    index_name="test_index2",
                    index_cols=("prim_key_sha256hash",),
                ),
            ],
        ),
        ("test_3", None, None),
    ),
)
def test_bootstrap(
    table_name,
    create_table_params,
    create_indexes_params,
    setup_test_db_conn: sqlite3.Connection,
):
    class _ORM(SimpleTableORM):
        orm_bootstrap_table_name = table_name
        if create_table_params:
            orm_bootstrap_create_table_params = create_table_params

        if create_indexes_params:
            orm_bootstrap_indexes_params = create_indexes_params

    _orm = _ORM(setup_test_db_conn)
    _orm.orm_bootstrap_db()


def _dummy_row_factory(_cur, _row) -> Any:
    return


def _compare_callable(left_func, right_func) -> bool:
    """Especially handling the"""
    _l_to_compare, _r_to_compare = left_func, right_func
    if isinstance(left_func, functools.partial) and isinstance(
        right_func, functools.partial
    ):
        return (
            left_func.func == right_func.func
            and left_func.args == right_func.args
            and left_func.keywords == right_func.keywords
        )
    return left_func == right_func


@pytest.mark.parametrize(
    "_row_factory_specifier, _expected_row_factory",
    (
        ("sqlite3_row_factory", sqlite3.Row),
        ("table_spec", SimpleTableForTest.table_row_factory),
        (
            "table_spec_no_validation",
            functools.partial(SimpleTableForTest.table_row_factory, validation=False),
        ),
        (DO_NOT_CHANGE_ROW_FACTORY, _dummy_row_factory),
        (None, None),
    ),
)
def test_row_factory_specifying(
    _row_factory_specifier,
    _expected_row_factory,
    db_conn_func_scope: sqlite3.Connection,
):
    db_conn_func_scope.row_factory = _dummy_row_factory
    _orm = SimpleTableORM(db_conn_func_scope, row_factory=_row_factory_specifier)
    _orm.orm_create_table()
    assert _compare_callable(_orm.orm_conn_row_factory, _expected_row_factory)

    # by the way, test the orm_conn_row_factory setter
    _orm.orm_conn_row_factory = _dummy_row_factory
    assert _orm.orm_conn_row_factory == _dummy_row_factory
