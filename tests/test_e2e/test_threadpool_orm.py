from __future__ import annotations

import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

import pytest

from simple_sqlite3_orm._orm import ORMThreadPoolBase
from simple_sqlite3_orm.utils import batched
from tests.conftest import INDEX_KEYS, INDEX_NAME, TABLE_NAME, TEST_INSERT_BATCH_SIZE
from tests.sample_db.table import SampleTable

logger = logging.getLogger(__name__)


class SampleDBConnectionPool(ORMThreadPoolBase[SampleTable]):
    """Test connection pool."""

    _orm_table_name = TABLE_NAME


THREAD_NUM = 2
WORKER_NUM = 6


class TestWithSampleDBAndThreadPool:
    @pytest.fixture(autouse=True, scope="class")
    def thread_pool(self, setup_con_factory: Callable[[], sqlite3.Connection]):
        with SampleDBConnectionPool(
            con_factory=setup_con_factory,
            number_of_cons=THREAD_NUM,
        ) as pool:
            yield pool

    def test_create_table(self, thread_pool: SampleDBConnectionPool):
        logger.info("test create table")
        thread_pool.orm_create_table(without_rowid=True)

    def test_insert_entries_with_pool(
        self,
        thread_pool: SampleDBConnectionPool,
        setup_test_data: dict[str, SampleTable],
    ):
        logger.info("test insert entries...")

        # simulating multiple worker threads submitting to database with access serialized.
        with ThreadPoolExecutor(max_workers=WORKER_NUM) as pool:
            for _batch_count, entry in enumerate(
                batched(setup_test_data.values(), TEST_INSERT_BATCH_SIZE),
                start=1,
            ):
                pool.submit(thread_pool.orm_insert_entries, entry)

            logger.info(
                f"all insert tasks are dispatched: {_batch_count} batches with {TEST_INSERT_BATCH_SIZE=}"
            )

        logger.info("confirm data written")
        for _selected_entry_count, _entry in enumerate(
            thread_pool.orm_select_entries(), start=1
        ):
            _corresponding_item = setup_test_data[_entry.prim_key]
            assert _corresponding_item == _entry
        assert _selected_entry_count == len(setup_test_data)

    def test_create_index(self, thread_pool: SampleDBConnectionPool):
        logger.info("test create index")
        thread_pool.orm_create_index(
            index_name=INDEX_NAME,
            index_keys=INDEX_KEYS,
            unique=True,
        )

    def test_orm_execute(
        self,
        thread_pool: SampleDBConnectionPool,
        setup_test_data: dict[str, SampleTable],
    ):
        logger.info("test orm_execute to check inserted entries num")
        sql_stmt = thread_pool.orm_table_spec.table_select_stmt(
            select_from=thread_pool.orm_table_name,
            function="count",
        )
        res: list[tuple[int]] = thread_pool.orm_execute(sql_stmt)

        assert res and res[0][0] == len(setup_test_data)

    def test_lookup_entries(
        self, thread_pool: SampleDBConnectionPool, entries_to_lookup: list[SampleTable]
    ):
        logger.info("test lookup entries")
        for _entry in entries_to_lookup:
            _looked_up = thread_pool.orm_select_entries(
                key_id=_entry.key_id,
                prim_key_sha256hash=_entry.prim_key_sha256hash,
            )
            _looked_up = list(_looked_up)
            assert len(_looked_up) == 1
            assert _looked_up[0] == _entry

    def test_delete_entries(
        self, thread_pool: SampleDBConnectionPool, entries_to_remove: list[SampleTable]
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
                _res = thread_pool.orm_delete_entries(
                    key_id=entry.key_id,
                    prim_key_sha256hash=entry.prim_key_sha256hash,
                    # _limit=1,
                )
                assert _res == 1
        else:
            for entry in entries_to_remove:
                _res = thread_pool.orm_delete_entries_with_returning(
                    _returning_cols="*",
                    key_id=entry.key_id,
                    prim_key_sha256hash=entry.prim_key_sha256hash,
                    # _limit=1,
                )
                _res_list = list(_res)

                assert len(_res_list) == 1
                assert _res_list[0] == entry
