"""Test with single thread and single connection."""

from __future__ import annotations

import logging
import sqlite3
from typing import Generator

import pytest

from simple_sqlite3_orm import utils
from tests.conftest import (
    INDEX_KEYS,
    INDEX_NAME,
    SELECT_ALL_BATCH_SIZE,
    TEST_INSERT_BATCH_SIZE,
)
from tests.sample_db.orm import SampleDB
from tests.sample_db.table import SampleTable

logger = logging.getLogger(__name__)


class TestWithSampleDB:
    @pytest.fixture(autouse=True)
    def setup_test(
        self,
        setup_test_db: SampleDB,
    ):
        self.orm_inst = setup_test_db

    def test_create_table(self):
        logger.info("test create table")
        self.orm_inst.orm_create_table()
        assert utils.lookup_table(self.orm_inst.orm_con, self.orm_inst.orm_table_name)

    def test_insert_entries(self, setup_test_data: dict[str, SampleTable]):
        logger.info("test insert entries")

        for entry in utils.batched(setup_test_data.values(), TEST_INSERT_BATCH_SIZE):
            self.orm_inst.orm_insert_entries(entry)

        logger.info("confirm data written")
        for _entry in self.orm_inst.orm_select_entries():
            _corresponding_item = setup_test_data[_entry.prim_key]
            assert _corresponding_item == _entry

        logger.info("confirm the num of inserted entries")
        with self.orm_inst.orm_con as _con:
            _cur = _con.execute(
                self.orm_inst.orm_table_spec.table_select_stmt(
                    select_from=self.orm_inst.orm_table_name,
                    function="count",
                )
            )
            _raw = _cur.fetchone()
            assert _raw[0] == len(setup_test_data)

    def test_create_index(self):
        logger.info("test create index")
        self.orm_inst.orm_create_index(
            index_name=INDEX_NAME,
            index_keys=INDEX_KEYS,
            unique=True,
        )

    def test_select_one_entry(
        self, setup_test_data: dict[str, SampleTable], entry_to_lookup: SampleTable
    ):
        logger.info("test select exactly one entry")
        assert entry_to_lookup == self.orm_inst.orm_select_entry(
            key_id=entry_to_lookup.key_id
        )

    def test_orm_execute(self, setup_test_data: dict[str, SampleTable]):
        logger.info("test orm_execute API")
        sql_stmt = self.orm_inst.orm_table_spec.table_select_stmt(
            select_from=self.orm_inst.orm_table_name,
            select_cols="*",
            function="count",
        )
        res = self.orm_inst.orm_execute(sql_stmt)
        assert res and res[0][0] == len(setup_test_data)

    def test_lookup_entries(self, entries_to_lookup: list[SampleTable]):
        logger.info("test lookup entries")
        for _entry in entries_to_lookup:
            _looked_up = self.orm_inst.orm_select_entries(
                key_id=_entry.key_id,
                prim_key_sha256hash=_entry.prim_key_sha256hash,
            )
            _looked_up = list(_looked_up)
            assert len(_looked_up) == 1
            assert _looked_up[0] == _entry

    def test_select_all_entries(self, setup_test_data: dict[str, SampleTable]):
        logger.info("test lookup entries")

        _looked_up = set()
        for _entry in self.orm_inst.orm_select_all_with_pagination(
            batch_size=SELECT_ALL_BATCH_SIZE
        ):
            assert setup_test_data[_entry.prim_key] == _entry
            _looked_up.add(_entry)

        assert len(_looked_up) == len(setup_test_data)
        assert all(_entry in _looked_up for _entry in setup_test_data.values())

    def test_delete_entries(
        self,
        setup_test_data: dict[str, SampleTable],
        entries_to_remove: list[SampleTable],
    ):
        logger.info("test remove and confirm the removed entries")
        if sqlite3.sqlite_version_info < (3, 35, 0):
            logger.warning(
                (
                    "Current runtime sqlite3 lib version doesn't support RETURNING statement:"
                    f"{sqlite3.version_info=}, needs 3.35 and above. "
                    "The test of RETURNING statement will be skipped here."
                )
            )

            for entry in entries_to_remove:
                _res = self.orm_inst.orm_delete_entries(
                    key_id=entry.key_id,
                    prim_key_sha256hash=entry.prim_key_sha256hash,
                    # NOTE(20241230): limit on update/delete requires compile flag SQLITE_ENABLE_UPDATE_DELETE_LIMIT enabled,
                    #   which is not set to be enabled by default. See https://www.sqlite.org/compile.html for more details.
                    # _limit=1,
                )
                assert _res == 1
        else:
            for entry in entries_to_remove:
                _res = self.orm_inst.orm_delete_entries_with_returning(
                    _returning_cols="*",
                    key_id=entry.key_id,
                    prim_key_sha256hash=entry.prim_key_sha256hash,
                    # _limit=1,
                )
                assert isinstance(_res, Generator)

                _res = list(_res)
                assert len(_res) == 1
                assert _res[0] == entry

        logger.info("confirm the remove")
        sql_stmt = self.orm_inst.orm_table_spec.table_select_stmt(
            select_from=self.orm_inst.orm_table_name,
            function="count",
        )

        res = self.orm_inst.orm_execute(sql_stmt)
        assert res and res[0][0] == len(setup_test_data) - len(entries_to_remove)
