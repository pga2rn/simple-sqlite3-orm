from __future__ import annotations

import asyncio
import logging
import random
import sqlite3
import time
from typing import Callable

import pytest
import pytest_asyncio

from simple_sqlite3_orm._orm._pool import _wrap_generator_with_async_ctx
from simple_sqlite3_orm.utils import batched
from tests.conftest import SQLITE3_COMPILE_OPTION_FLAGS
from tests.sample_db.orm import SampleDB, SampleDBAsyncio
from tests.sample_db.table import SampleTable, SampleTableCols
from tests.test_e2e.conftest import (
    INDEX_KEYS,
    INDEX_NAME,
    TEST_ENTRY_NUM,
    TEST_INSERT_BATCH_SIZE,
)

logger = logging.getLogger(__name__)

THREAD_NUM = 2
# NOTE: the timer interval should not be smaller than 0.01 due to the precision
#   of asyncio internal clock.
TIMER_INTERVAL = 0.1


@pytest.mark.asyncio(loop_scope="class")
class TestWithSampleDBWithAsyncIO:
    @pytest_asyncio.fixture(scope="class", loop_scope="class")
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
        _batch_count = 0
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

    async def test_lookup_entries(
        self, async_pool: SampleDBAsyncio, entries_to_lookup: list[SampleTable]
    ):
        logger.info("test lookup entries")
        for _entry in entries_to_lookup:
            _looked_up = await async_pool.orm_select_entries(
                SampleTableCols(
                    key_id=_entry.key_id, prim_key_sha256hash=_entry.prim_key_sha256hash
                ),
            )

            _looked_up_list = []
            async for _item in _looked_up:
                _looked_up_list.append(_item)

            assert len(_looked_up_list) == 1
            assert _looked_up_list[0] == _entry

    async def test_caller_exits_when_lookup_entries(self, async_pool: SampleDBAsyncio):
        class _StopAt(Exception): ...

        async def _wrapper():
            _stop_at = random.randrange(TEST_ENTRY_NUM // 2, TEST_ENTRY_NUM)

            _count = 0
            async for _entry in await async_pool.orm_select_entries():
                _count += 1
                if _count >= _stop_at:
                    raise _StopAt("stop as expected")
                yield _entry

        with pytest.raises(_StopAt):
            async for _ in _wrapper():
                ...

    async def test_in_thread_raise_exceptions_when_lookup_entries(
        self, async_pool: SampleDBAsyncio
    ):
        _origin_lookup_entries = SampleDB.orm_select_entries
        _stop_at = random.randrange(TEST_ENTRY_NUM // 2, TEST_ENTRY_NUM)

        class _StopAt(Exception): ...

        def _mocked_select_entries(*args, **kwargs):
            for _count, _entry in enumerate(_origin_lookup_entries(*args, **kwargs)):
                if _count >= _stop_at:
                    raise _StopAt("stop as expected")
                yield _entry

        # NOTE: bound the wrapped to async_pool
        _wrapped_mocked_select_entries = _wrap_generator_with_async_ctx(
            _mocked_select_entries
        ).__get__(async_pool)

        with pytest.raises(_StopAt):
            async for _ in await _wrapped_mocked_select_entries():
                ...

    async def test_delete_entries(
        self, async_pool: SampleDBAsyncio, entries_to_remove: list[SampleTable]
    ):
        logger.info("test remove and confirm the removed entries")
        if SQLITE3_COMPILE_OPTION_FLAGS.RETURNING_AVAILABLE:
            logger.warning(
                (
                    "Current runtime sqlite3 lib version doesn't support RETURNING statement:"
                    f"{sqlite3.sqlite_version_info=}, needs 3.35 and above. "
                    "The test of RETURNING statement will be skipped here."
                )
            )

            for entry in entries_to_remove:
                _res = await async_pool.orm_delete_entries(
                    SampleTableCols(
                        key_id=entry.key_id,
                        prim_key_sha256hash=entry.prim_key_sha256hash,
                    ),
                    # _limit=1,
                )
                assert _res == 1
        else:
            for entry in entries_to_remove:
                _res = await async_pool.orm_delete_entries_with_returning(
                    SampleTableCols(
                        key_id=entry.key_id,
                        prim_key_sha256hash=entry.prim_key_sha256hash,
                    ),
                    _returning_cols="*",
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
