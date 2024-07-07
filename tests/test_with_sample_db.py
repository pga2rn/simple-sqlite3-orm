"""Test with single thread and single connection."""

from __future__ import annotations

import logging
import random
import sqlite3
from typing import Generator

import pytest

from simple_sqlite3_orm import utils
from tests.conftest import (
    TABLE_NAME,
    TEST_ENTRY_NUM,
    TEST_LOOKUP_ENTRIES_NUM,
    TEST_REMOVE_ENTRIES_NUM,
    generate_test_data,
)
from tests.sample_db.orm import SampleDB
from tests.sample_db.table import SampleTable

logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def setup_test_data():
    return generate_test_data(TEST_ENTRY_NUM)


@pytest.fixture(scope="class")
def setup_test_db(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[SampleDB, None, None]:
    tmp_path = tmp_path_factory.mktemp("tmp_db_path")
    db_file = tmp_path / "test_db_file.sqlite3"

    con = sqlite3.connect(db_file)

    # enable optimization
    utils.enable_wal_mode(con, relax_sync_mode=True)
    utils.enable_mmap(con)
    utils.enable_tmp_store_at_memory(con)
    yield SampleDB(con, table_name=TABLE_NAME)
    # finally, do a database integrity check after test operations
    assert utils.check_db_integrity(con)


@pytest.fixture(scope="class")
def entries_to_lookup(setup_test_data: dict[str, SampleTable]) -> list[SampleTable]:
    return random.sample(
        list(setup_test_data.values()),
        k=TEST_LOOKUP_ENTRIES_NUM,
    )


@pytest.fixture(scope="class")
def entries_to_remove(setup_test_data: dict[str, SampleTable]) -> list[SampleTable]:
    return random.sample(
        list(setup_test_data.values()),
        k=TEST_REMOVE_ENTRIES_NUM,
    )


INDEX_NAME = "key_id_prim_key_hash_idx"
INDEX_KEYS = ("key_id", "prim_key_sha256hash")


class TestWithSampleDB:

    @pytest.fixture(autouse=True)
    def setup_test(
        self,
        setup_test_db: SampleDB,
        setup_test_data: dict[str, SampleTable],
        entries_to_lookup: list[SampleTable],
        entries_to_remove: list[SampleTable],
    ):
        self.table_name = TABLE_NAME

        self.data_for_test = setup_test_data
        self.table_spec = SampleTable
        self.orm_inst = setup_test_db
        self.data_len = len(setup_test_data)
        self.entries_to_lookup = entries_to_lookup
        self.entries_to_remove = entries_to_remove

    def test_create_table(self):
        logger.info("test create table")
        self.orm_inst.orm_create_table(without_rowid=True)
        assert utils.lookup_table(self.orm_inst.orm_con, self.table_name)

    def test_create_index(self):
        logger.info("test create index")
        self.orm_inst.orm_create_index(
            index_name=INDEX_NAME,
            index_keys=INDEX_KEYS,
            unique=True,
        )

    def test_insert_entries(self):
        logger.info("test insert entries")
        self.orm_inst.orm_insert_entries(self.data_for_test.values())

        logger.info("confirm data written")
        for _entry in self.orm_inst.orm_select_entries():
            _corresponding_item = self.data_for_test[_entry.prim_key]
            assert _corresponding_item == _entry

        logger.info("confirm the num of inserted entries")
        with self.orm_inst.orm_con as _con:
            _cur = _con.execute(
                self.table_spec.table_select_stmt(
                    select_from=self.table_name, function="count"
                )
            )
            _raw = _cur.fetchone()
            assert _raw[0] == self.data_len

    def test_lookup_entries(self):
        logger.info("test lookup entries")
        for _entry in self.entries_to_lookup:
            _looked_up = self.orm_inst.orm_select_entries(
                key_id=_entry.key_id,
                prim_key_sha256hash=_entry.prim_key_sha256hash,
            )
            _looked_up = list(_looked_up)
            assert len(_looked_up) == 1
            assert _looked_up[0] == _entry

    def test_delete_entries(self):
        logger.info("test remove and confirm the removed entries")
        if sqlite3.sqlite_version_info < (3, 35, 0):
            logger.warning(
                (
                    "Current runtime sqlite3 lib version doesn't support RETURNING statement:"
                    f"{sqlite3.version_info=}, needs 3.35 and above. "
                    "The test of RETURNING statement will be skipped here."
                )
            )

            for entry in self.entries_to_remove:
                _res = self.orm_inst.orm_delete_entries(
                    key_id=entry.key_id,
                    prim_key_sha256hash=entry.prim_key_sha256hash,
                )
                assert _res == 1
        else:
            for entry in self.entries_to_remove:
                _res = self.orm_inst.orm_delete_entries(
                    _returning_cols="*",
                    key_id=entry.key_id,
                    prim_key_sha256hash=entry.prim_key_sha256hash,
                )
                assert isinstance(_res, Generator)

                _res = list(_res)
                assert len(_res) == 1
                assert _res[0] == entry

        logger.info("confirm the remove")
        with self.orm_inst.orm_con as _con:
            _cur = _con.execute(
                self.table_spec.table_select_stmt(
                    select_from=self.table_name, function="count"
                )
            )
            _raw = _cur.fetchone()
            assert _raw[0] == self.data_len - len(self.entries_to_remove)
