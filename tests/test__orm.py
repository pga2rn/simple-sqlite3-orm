from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime

import pytest

from simple_sqlite3_orm import CreateIndexParams, CreateTableParams
from tests.conftest import SELECT_ALL_BATCH_SIZE, _generate_random_str
from tests.sample_db._types import Mystr
from tests.sample_db.orm import SampleDB
from tests.sample_db.table import SampleTable

logger = logging.getLogger(__name__)


_cur_timestamp = time.time()
mstr = Mystr(_generate_random_str())
entry_for_test = SampleTable(
    unix_timestamp=_cur_timestamp,  # type: ignore
    unix_timestamp_int=int(_cur_timestamp),  # type: ignore
    datetime_iso8601=datetime.fromtimestamp(_cur_timestamp).isoformat(),  # type: ignore
    key_id=1,
    prim_key=mstr,
    prim_key_sha256hash=mstr.sha256hash,
    prim_key_bln=mstr.bool,
    prim_key_magicf=mstr.magicf,
)

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
            orm_inst = SampleDB(conn)
            yield orm_inst

    def test_create_without_rowid_table(self):
        """NOTE: to test select_all_with_pagination, we cannot specify without_rowid, so we
        create this test case dedicated for creating without_rowid table test."""
        with sqlite3.connect(":memory:") as conn:
            orm_inst = SampleDB(conn)
            orm_inst.orm_create_table(without_rowid=True)

    def test_create_table(self, setup_connection: SampleDB):
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

    def test_create_index(self, setup_connection: SampleDB):
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

    def test_insert_entries(self, setup_connection: SampleDB):
        setup_connection.orm_insert_entries((entry_for_test,))

        with pytest.raises(sqlite3.DatabaseError):
            setup_connection.orm_insert_entry(entry_for_test, or_option="fail")
        setup_connection.orm_insert_entry(entry_for_test, or_option="ignore")
        setup_connection.orm_insert_entry(entry_for_test, or_option="replace")

    def test_orm_execute(self, setup_connection: SampleDB):
        sql_stmt = setup_connection.orm_table_spec.table_select_stmt(
            select_from=setup_connection.orm_table_name,
            select_cols="*",
            function="count",
        )

        res = setup_connection.orm_execute(sql_stmt)
        assert res and res[0][0] > 0

    def test_orm_check_entry_exist(self, setup_connection: SampleDB):
        assert setup_connection.orm_check_entry_exist(prim_key=entry_for_test.prim_key)
        assert not setup_connection.orm_check_entry_exist(prim_key=Mystr("not_exist"))

    def test_select_entry(self, setup_connection: SampleDB):
        _selected_row = setup_connection.orm_select_entry(prim_key=mstr)
        assert entry_for_test == _selected_row

    def test_select_entries(self, setup_connection: SampleDB):
        select_result = setup_connection.orm_select_entries(
            _distinct=True,
            _order_by=(("key_id", "DESC"),),
            _limit=1,
            prim_key=mstr,
        )
        select_result = list(select_result)

        assert len(select_result) == 1
        assert select_result[0] == entry_for_test

    def test_select_all_entries(self, setup_connection: SampleDB):
        select_result = setup_connection.orm_select_all_with_pagination(
            batch_size=SELECT_ALL_BATCH_SIZE
        )
        select_result = list(select_result)

        assert len(select_result) == 1
        assert select_result[0] == entry_for_test

    def test_function_call(self, setup_connection: SampleDB):
        with setup_connection.orm_con as con:
            cur = con.execute(f"SELECT count(*) FROM {SampleDB._orm_table_name};")
            res = cur.fetchone()
            assert res[0] == 1

    def test_delete_entries(self, setup_connection: SampleDB):
        assert setup_connection.orm_delete_entries(key_id=entry_for_test.key_id) == 1


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
    class _ORM(SampleDB):
        orm_bootstrap_table_name = table_name
        if create_table_params:
            orm_bootstrap_create_table_params = create_table_params

        if create_indexes_params:
            orm_bootstrap_indexes_params = create_indexes_params

    _orm = _ORM(setup_test_db_conn)
    _orm.orm_bootstrap_db()
