from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from typing import Callable

import pytest
import pytest_asyncio

from simple_sqlite3_orm._orm import AsyncORMConnectionThreadPool
from simple_sqlite3_orm.utils import batched
from tests.sample_db.table import SampleTable
from tests.test_with_sample_db import INDEX_KEYS, INDEX_NAME, TABLE_NAME

logger = logging.getLogger(__name__)

THREAD_NUM = 2
TIMER_INTERVAL = 0.01
BLOCKING_FACTOR = 1.2


class SampleDBAsyncio(AsyncORMConnectionThreadPool[SampleTable]):
    """Test connection pool with async API."""


@pytest.mark.asyncio(scope="class")
class TestWithSampleDBWithAsyncIO:

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

    @pytest_asyncio.fixture(scope="class")
    @pytest.mark.asyncio(scope="class")
    async def async_pool(
        self,
        setup_con_factory: Callable[[], sqlite3.Connection],
    ):
        try:
            pool = SampleDBAsyncio(
                TABLE_NAME,
                con_factory=setup_con_factory,
                number_of_cons=THREAD_NUM,
            )
            yield pool
        finally:
            pool.orm_pool_shutdown()

    @pytest_asyncio.fixture(autouse=True, scope="class")
    @pytest.mark.asyncio(scope="class")
    async def start_timer(self) -> tuple[asyncio.Task[None], asyncio.Event]:
        _test_finished = asyncio.Event()

        async def _timer():
            count = 0

            start_time = time.time()
            while not _test_finished.is_set():
                await asyncio.sleep(TIMER_INTERVAL)
                count += 1

            total_time_cost = TIMER_INTERVAL * count
            actual_time_cost = time.time() - start_time

            logger.info(f"{total_time_cost=}, {actual_time_cost=}")
            assert actual_time_cost <= total_time_cost * BLOCKING_FACTOR

        return asyncio.create_task(_timer()), _test_finished

    async def test_create_table(self, async_pool: SampleDBAsyncio):
        logger.info("test create table")
        await async_pool.orm_create_table(without_rowid=True)

    async def test_insert_entries_with_pool(self, async_pool: SampleDBAsyncio):
        logger.info("test insert entries...")

        _BATCH_SIZE = 128
        _tasks = []
        for _batch_count, entry in enumerate(
            batched(self.data_for_test.values(), _BATCH_SIZE),
            start=1,
        ):
            _task = asyncio.create_task(async_pool.orm_insert_entries(entry))
            _tasks.append(_task)

        logger.info(
            f"all insert tasks are dispatched: {_batch_count} batches with {_BATCH_SIZE=}"
        )
        for _fut in asyncio.as_completed(_tasks):
            await _fut

        logger.info("confirm data written with orm_select_entries_gen")
        _count = 0
        async for _entry in async_pool.orm_select_entries_gen():
            _corresponding_item = self.data_for_test[_entry.prim_key]
            assert _corresponding_item == _entry
            _count += 1
        assert _count == len(self.data_for_test)

    async def test_create_index(self, async_pool: SampleDBAsyncio):
        logger.info("test create index")
        await async_pool.orm_create_index(
            index_name=INDEX_NAME,
            index_keys=INDEX_KEYS,
            unique=True,
        )

    async def test_confirm_not_blocking(
        self, start_timer: tuple[asyncio.Task[None], asyncio.Event]
    ) -> None:
        """Confirm that during the ORM async API call, the main event loop is not blocked."""
        _task, _event = start_timer
        _event.set()
        await _task
