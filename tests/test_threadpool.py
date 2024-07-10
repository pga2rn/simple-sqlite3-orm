from __future__ import annotations

import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

import pytest

from simple_sqlite3_orm._orm import ORMConnectionThreadPool
from simple_sqlite3_orm.utils import batched
from tests.sample_db.orm import SampleDB
from tests.sample_db.table import SampleTable
from tests.test_with_sample_db import INDEX_KEYS, INDEX_NAME, TABLE_NAME

logger = logging.getLogger(__name__)


class SampleDBConnectionPool(ORMConnectionThreadPool[SampleTable]):
    """Test connection pool."""


THREAD_NUM = 2
WORKER_NUM = 6


class TestWithSampleDBAndThreadPool:

    @pytest.fixture(autouse=True)
    def setup_test(
        self,
        setup_con_factory: Callable[[], sqlite3.Connection],
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

        self.orm_inst = SampleDB(setup_con_factory(), TABLE_NAME)
        self.pool = SampleDBConnectionPool(
            TABLE_NAME,
            con_factory=setup_con_factory,
            number_of_cons=THREAD_NUM,
        )

    def test_create_table(self):
        logger.info("test create table")
        self.orm_inst.orm_create_table(without_rowid=True)

    def test_insert_entries_with_pool(self):
        logger.info("test insert entries...")

        _BATCH_SIZE = 128
        futs = []
        # simulating multiple worker threads submitting to database with access serialized.
        with ThreadPoolExecutor(max_workers=WORKER_NUM) as pool:
            for _batch_count, entry in enumerate(
                batched(self.data_for_test.values(), _BATCH_SIZE),
                start=1,
            ):
                futs.append(pool.submit(self.pool.orm_insert_entries, entry))

            logger.info(
                f"all insert tasks are dispatched: {_batch_count} batches with {_BATCH_SIZE=}"
            )
            for fut in as_completed(futs):
                fut.result()

        logger.info("confirm data written")
        for _selected_entry_count, _entry in enumerate(
            self.pool.orm_select_entries(_return_as_generator=True), start=1
        ):
            _corresponding_item = self.data_for_test[_entry.prim_key]
            assert _corresponding_item == _entry
        assert _selected_entry_count == len(self.data_for_test)

    def test_create_index(self):
        logger.info("test create index")
        self.orm_inst.orm_create_index(
            index_name=INDEX_NAME,
            index_keys=INDEX_KEYS,
            unique=True,
        )
