from __future__ import annotations

import contextlib
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
    ID_STR_DEFAULT_VALUE,
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
    """A quick test for testing functionality of ORM base."""

    @pytest.fixture
    def orm_inst(self):
        with contextlib.closing(sqlite3.connect(":memory:")) as conn:
            orm_inst = SimpleTableORM(conn)
            orm_inst.orm_bootstrap_db()
            yield orm_inst

    @pytest.fixture
    def prepare_test_entry(self, orm_inst: SimpleTableORM):
        orm_inst.orm_insert_entry(ENTRY_FOR_TEST)

    def test_create_index(self, orm_inst: SimpleTableORM):
        orm_inst.orm_create_index(
            index_name="id_str_index",
            index_keys=("id_str",),
            allow_existed=True,
            unique=True,
        )

        with pytest.raises(sqlite3.DatabaseError):
            orm_inst.orm_create_index(
                index_name="id_str_index",
                index_keys=("id_str",),
                allow_existed=False,
            )

    def test_insert_entries(self, orm_inst: SimpleTableORM):
        orm_inst.orm_insert_entries((ENTRY_FOR_TEST,))

        with pytest.raises(sqlite3.DatabaseError):
            orm_inst.orm_insert_entry(ENTRY_FOR_TEST, or_option="fail")
        orm_inst.orm_insert_entry(ENTRY_FOR_TEST, or_option="ignore")
        orm_inst.orm_insert_entry(ENTRY_FOR_TEST, or_option="replace")

    def test_orm_execute(self, orm_inst: SimpleTableORM, prepare_test_entry):
        sql_stmt = orm_inst.orm_table_spec.table_select_stmt(
            select_from=orm_inst.orm_table_name,
            select_cols="*",
            function="count",
        )

        res = orm_inst.orm_execute(sql_stmt)
        assert res and res[0][0] > 0

    def test_orm_check_entry_exist(self, orm_inst: SimpleTableORM, prepare_test_entry):
        assert orm_inst.orm_check_entry_exist(
            SimpleTableForTestCols(id=ENTRY_FOR_TEST.id)
        )
        assert orm_inst.orm_check_entry_exist(
            **SimpleTableForTestCols(int_str=ENTRY_FOR_TEST.int_str)
        )
        assert not orm_inst.orm_check_entry_exist(int_str="123")

    def test_select_entry(self, orm_inst: SimpleTableORM, prepare_test_entry):
        _selected_row = orm_inst.orm_select_entry(
            SimpleTableForTestCols(id=ENTRY_FOR_TEST.id)
        )
        _selected_row2 = orm_inst.orm_select_entry(
            **SimpleTableForTestCols(id=ENTRY_FOR_TEST.id)
        )
        assert ENTRY_FOR_TEST == _selected_row == _selected_row2

    @pytest.mark.parametrize(
        "row_as_mapping, expected",
        (
            (
                SimpleTableForTestCols(id=123, int_str=123, id_str="123"),
                SimpleTableForTestCols(id=123, int_str=123, id_str="123", extra=None),
            ),
            (
                SimpleTableForTestCols(int_str=123, id_str="456", extra=1.23),
                SimpleTableForTestCols(int_str=123, id_str="456", extra=1.23, id=1),
            ),
            (
                SimpleTableForTestCols(int_str=123),
                SimpleTableForTestCols(
                    int_str=123, id=1, id_str=ID_STR_DEFAULT_VALUE, extra=None
                ),
            ),
        ),
    )
    def test_insert_mapping(self, row_as_mapping, expected, orm_inst: SimpleTableORM):
        orm_inst.orm_insert_mapping(row_as_mapping)
        assert orm_inst.orm_select_entry(row_as_mapping).table_asdict() == expected

    @pytest.mark.parametrize(
        "row_as_mappings",
        (
            (
                [
                    SimpleTableForTestCols(id_str=str(i), int_str=i, extra=0.123)
                    for i in range(123)
                ]
            ),
            ([]),
        ),
    )
    def test_insert_mappings(self, row_as_mappings, orm_inst: SimpleTableORM):
        assert len(row_as_mappings) == orm_inst.orm_insert_mappings(row_as_mappings)

        for row_as_mapping in row_as_mappings:
            assert (
                orm_inst.orm_select_entry(row_as_mapping).table_asdict(*row_as_mapping)
                == row_as_mapping
            )

    def test_select_entries(self, orm_inst: SimpleTableORM, prepare_test_entry):
        select_result = orm_inst.orm_select_entries(
            SimpleTableForTestCols(id=ENTRY_FOR_TEST.id),
            _distinct=True,
            _order_by=(("id", "DESC"),),
            _limit=1,
        )
        select_result = list(select_result)

        select_result2 = orm_inst.orm_select_entries(
            **SimpleTableForTestCols(id=ENTRY_FOR_TEST.id),
            _distinct=True,
            _order_by=(("id", "DESC"),),
            _limit=1,
        )
        select_result2 = list(select_result2)

        assert len(select_result) == len(select_result2) == 1
        assert select_result[0] == select_result2[0] == ENTRY_FOR_TEST

    def test_select_all_entries(self, orm_inst: SimpleTableORM, prepare_test_entry):
        select_result = orm_inst.orm_select_all_with_pagination(
            batch_size=SELECT_ALL_BATCH_SIZE
        )
        select_result = list(select_result)

        assert len(select_result) == 1
        assert select_result[0] == ENTRY_FOR_TEST

    def test_select_with_function_call(
        self, orm_inst: SimpleTableORM, prepare_test_entry
    ):
        _stmt = orm_inst.orm_table_spec.table_select_stmt(
            select_from=orm_inst.orm_bootstrap_table_name,
            select_cols="*",
            function="count",
        )

        with orm_inst.orm_con as con:
            cur = con.execute(_stmt)
            res = cur.fetchone()
            assert res[0] == 1

    @pytest.mark.parametrize(
        "entry_to_insert, set_values, where_indicator, expected",
        (
            (
                SimpleTableForTestCols(id_str="123", extra=None, int_str=789),
                SimpleTableForTestCols(extra=0.123),
                SimpleTableForTestCols(id=1),
                SimpleTableForTestCols(id=1, id_str="123", extra=0.123, int_str=789),
            ),
            (
                SimpleTableForTestCols(id_str="123", extra=None, int_str=789, id=567),
                SimpleTableForTest(id_str="123", extra=0.123, int_str=789, id=1),
                SimpleTableForTestCols(id=567),
                SimpleTableForTestCols(id=1, id_str="123", extra=0.123, int_str=789),
            ),
        ),
    )
    def test_update_entries(
        self,
        entry_to_insert,
        set_values,
        where_indicator,
        expected,
        orm_inst: SimpleTableORM,
    ):
        orm_inst.orm_insert_mapping(entry_to_insert)

        if isinstance(where_indicator, str):
            orm_inst.orm_update_entries(
                set_values=set_values, where_stmt=where_indicator
            )
        else:
            orm_inst.orm_update_entries(
                set_values=set_values, where_cols_value=where_indicator
            )
        assert orm_inst.orm_check_entry_exist(expected)

    @pytest.mark.parametrize(
        "entry_to_insert, set_values, where_stmt, params, expected",
        (
            (
                SimpleTableForTest(id_str="123", extra=None, int_str=789, id=567),
                SimpleTableForTestCols(extra=0.123),
                "WHERE id > :lower_bound AND id < :upper_bound",
                {"lower_bound": 1, "upper_bound": 987},
                SimpleTableForTestCols(id=567, id_str="123", extra=0.123, int_str=789),
            ),
        ),
    )
    def test_update_entries_with_custom_stmt(
        self,
        entry_to_insert,
        set_values,
        where_stmt,
        params,
        expected,
        orm_inst: SimpleTableORM,
    ):
        orm_inst.orm_insert_entry(entry_to_insert)

        orm_inst.orm_update_entries(
            set_values=set_values,
            where_stmt=where_stmt,
            _extra_params=params,
        )
        assert orm_inst.orm_check_entry_exist(expected)

    def test_delete_entries(self, orm_inst: SimpleTableORM, prepare_test_entry):
        assert (
            orm_inst.orm_delete_entries(SimpleTableForTestCols(id=ENTRY_FOR_TEST.id))
            == 1
        )


@pytest.mark.parametrize(
    "opts",
    (
        ({"allow_existed": True, "without_rowid": True}),
        ({"without_rowid": False}),
        ({"_stmt": SimpleTableForTest.table_create_stmt(table_name=TBL_NAME)}),
    ),
)
def test_create_table(opts):
    with contextlib.closing(sqlite3.connect(":memory:")) as conn:
        orm_inst = SimpleTableORM(conn)
        orm_inst.orm_create_table(**opts)


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
                    index_cols=("id_str",),
                    if_not_exists=True,
                    unique=True,
                ),
                CreateIndexParams(
                    index_name="test_index2",
                    index_cols=("int_str",),
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
