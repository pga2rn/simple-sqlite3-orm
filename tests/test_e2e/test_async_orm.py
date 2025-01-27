from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from typing import Callable

import pytest
import pytest_asyncio

from simple_sqlite3_orm._orm import AsyncORMBase
from simple_sqlite3_orm.utils import batched
from tests.conftest import INDEX_KEYS, INDEX_NAME, TABLE_NAME, TEST_INSERT_BATCH_SIZE
from tests.sample_db.table import SampleTable

logger = logging.getLogger(__name__)

THREAD_NUM = 2
# NOTE: the timer interval should not be smaller than 0.01 due to the precision
#   of asyncio internal clock.
TIMER_INTERVAL = 0.1


class SampleDBAsyncio(AsyncORMBase[SampleTable]):
    """Test connection pool with async API."""

    _orm_table_name = TABLE_NAME


@pytest.mark.asyncio(loop_scope="class")
class TestWithSampleDBWithAsyncIO:
    @pytest_asyncio.fixture(loop_scope="class")
    async def async_pool(
        self,
        setup_con_factory: Callable[[], sqlite3.Connection],
    ):
        with SampleDBAsyncio(
            con_factory=setup_con_factory,
            number_of_cons=THREAD_NUM,
        ) as pool:
            yield pool

    @pytest_asyncio.fixture(autouse=True, scope="class")
    async def start_timer(self) -> tuple[asyncio.Task[None], asyncio.Event]:
        _test_finished = asyncio.Event()

        async def _timer():
            count = 0

            start_time = time.time()
            while not _test_finished.is_set():
                await asyncio.sleep(TIMER_INTERVAL)
                count += 1

            actual_time_cost = time.time() - start_time
            total_time_cost = TIMER_INTERVAL * count

            logger.info(f"{count=}, {total_time_cost=}, {actual_time_cost=}")

        return asyncio.create_task(_timer()), _test_finished

    async def test_create_table(self, async_pool: SampleDBAsyncio):
        logger.info("test create table")
        await async_pool.orm_create_table(without_rowid=True)

    async def test_insert_entries_with_pool(
        self, async_pool: SampleDBAsyncio, setup_test_data: dict[str, SampleTable]
    ):
        logger.info("test insert entries...")

        _tasks = []
        for _batch_count, entry in enumerate(
            batched(setup_test_data.values(), TEST_INSERT_BATCH_SIZE),
            start=1,
        ):
            _task = asyncio.create_task(async_pool.orm_insert_entries(entry))
            _tasks.append(_task)

        logger.info(
            f"all insert tasks are dispatched: {_batch_count} batches with {TEST_INSERT_BATCH_SIZE=}"
        )
        await asyncio.wait(_tasks)

        logger.info("confirm data written with orm_select_entries_gen")
        _count = 0
        async for _entry in await async_pool.orm_select_entries():
            _corresponding_item = setup_test_data[_entry.prim_key]
            assert _corresponding_item == _entry
            _count += 1
        assert _count == len(setup_test_data)

    async def test_create_index(self, async_pool: SampleDBAsyncio):
        logger.info("test create index")
        await async_pool.orm_create_index(
            index_name=INDEX_NAME,
            index_keys=INDEX_KEYS,
            unique=True,
        )

    async def test_lookup_entries(
        self, async_pool: SampleDBAsyncio, setup_test_data: dict[str, SampleTable]
    ):
        logger.info("test lookup entries")
        for _entry in setup_test_data.values():
            _looked_up = await async_pool.orm_select_entries(
                key_id=_entry.key_id,
                prim_key_sha256hash=_entry.prim_key_sha256hash,
            )

            _looked_up_list = []
            async for _item in _looked_up:
                _looked_up_list.append(_item)

            assert len(_looked_up_list) == 1
            assert _looked_up_list[0] == _entry

    async def test_orm_execute(
        self, async_pool: SampleDBAsyncio, setup_test_data: dict[str, SampleTable]
    ):
        logger.info("test orm_execute to check inserted entries num")
        sql_stmt = async_pool.orm_table_spec.table_select_stmt(
            select_from=async_pool.orm_table_name,
            function="count",
        )
        res = await async_pool.orm_execute(sql_stmt)

        assert res and res[0][0] == len(setup_test_data)

    async def test_delete_entries(
        self, async_pool: SampleDBAsyncio, entries_to_remove: list[SampleTable]
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
                _res = await async_pool.orm_delete_entries(
                    key_id=entry.key_id,
                    prim_key_sha256hash=entry.prim_key_sha256hash,
                    # _limit=1,
                )
                assert _res == 1
        else:
            for entry in entries_to_remove:
                _res = await async_pool.orm_delete_entries_with_returning(
                    _returning_cols="*",
                    key_id=entry.key_id,
                    prim_key_sha256hash=entry.prim_key_sha256hash,
                    # _limit=1,
                )

                _deleted_entry_list = []
                async for _item in _res:
                    _deleted_entry_list.append(_item)

                assert len(_deleted_entry_list) == 1
                assert _deleted_entry_list[0] == entry

    async def test_check_timer(
        self, start_timer: tuple[asyncio.Task[None], asyncio.Event]
    ) -> None:
        """Confirm that during the ORM async API call, the main event loop is not (so) blocked."""
        _task, _event = start_timer
        _event.set()
        await _task
