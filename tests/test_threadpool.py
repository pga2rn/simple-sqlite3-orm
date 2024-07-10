from __future__ import annotations

import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

import pytest

from simple_sqlite3_orm._orm import ORMConnectionThreadPool
from simple_sqlite3_orm.utils import batched
from tests.conftest import INDEX_KEYS, INDEX_NAME, TABLE_NAME, TEST_INSERT_BATCH_SIZE
from tests.sample_db.table import SampleTable

logger = logging.getLogger(__name__)


class SampleDBConnectionPool(ORMConnectionThreadPool[SampleTable]):
    """Test connection pool."""


THREAD_NUM = 2
WORKER_NUM = 6


class TestWithSampleDBAndThreadPool:

    @pytest.fixture(autouse=True)
    def setup_test(
        self,
        setup_test_data: dict[str, SampleTable],
        entries_to_lookup: list[SampleTable],
        entries_to_remove: list[SampleTable],
    ):
        self.table_name = TABLE_NAME

        self.data_for_test = setup_test_data
        self.table_spec = SampleTable
        self.data_len = len(setup_test_data)
        self.entries_to_lookup = entries_to_lookup
        self.entries_to_remove = entries_to_remove

    @pytest.fixture(autouse=True, scope="class")
    def thread_pool(self, setup_con_factory: Callable[[], sqlite3.Connection]):
        try:
            pool = SampleDBConnectionPool(
                TABLE_NAME,
                con_factory=setup_con_factory,
                number_of_cons=THREAD_NUM,
            )
            yield pool
        finally:
            pool.orm_pool_shutdown()

    def test_create_table(self, thread_pool: SampleDBConnectionPool):
        logger.info("test create table")
        thread_pool.orm_create_table(without_rowid=True)

    def test_insert_entries_with_pool(self, thread_pool: SampleDBConnectionPool):
        logger.info("test insert entries...")

        # simulating multiple worker threads submitting to database with access serialized.
        with ThreadPoolExecutor(max_workers=WORKER_NUM) as pool:
            for _batch_count, entry in enumerate(
                batched(self.data_for_test.values(), TEST_INSERT_BATCH_SIZE),
                start=1,
            ):
                pool.submit(thread_pool.orm_insert_entries, entry)

            logger.info(
                f"all insert tasks are dispatched: {_batch_count} batches with {TEST_INSERT_BATCH_SIZE=}"
            )

        logger.info("confirm data written")
        for _selected_entry_count, _entry in enumerate(
            thread_pool.orm_select_entries_gen(), start=1
        ):
            _corresponding_item = self.data_for_test[_entry.prim_key]
            assert _corresponding_item == _entry
        assert _selected_entry_count == len(self.data_for_test)

    def test_create_index(self, thread_pool: SampleDBConnectionPool):
        logger.info("test create index")
        thread_pool.orm_create_index(
            index_name=INDEX_NAME,
            index_keys=INDEX_KEYS,
            unique=True,
        )

    def test_lookup_entries(self, thread_pool: SampleDBConnectionPool):
        logger.info("test lookup entries")
        for _entry in self.entries_to_lookup:
            _looked_up = thread_pool.orm_select_entries(
                key_id=_entry.key_id,
                prim_key_sha256hash=_entry.prim_key_sha256hash,
            )
            _looked_up = list(_looked_up)
            assert len(_looked_up) == 1
            assert _looked_up[0] == _entry

    def test_delete_entries(self, thread_pool: SampleDBConnectionPool):
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
                _res = thread_pool.orm_delete_entries(
                    key_id=entry.key_id,
                    prim_key_sha256hash=entry.prim_key_sha256hash,
                )
                assert _res == 1
        else:
            for entry in self.entries_to_remove:
                _res = thread_pool.orm_delete_entries(
                    _returning_cols="*",
                    key_id=entry.key_id,
                    prim_key_sha256hash=entry.prim_key_sha256hash,
                )
                assert isinstance(_res, list)

                assert len(_res) == 1
                assert _res[0] == entry
