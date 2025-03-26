from __future__ import annotations

import logging
import random
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Callable

import pytest

from simple_sqlite3_orm._orm._pool import _wrap_generator_with_thread_ctx
from simple_sqlite3_orm.utils import batched
from tests.conftest import SQLITE3_COMPILE_OPTION_FLAGS
from tests.sample_db.orm import SampleDB, SampleDBConnectionPool
from tests.sample_db.table import SampleTable, SampleTableCols
from tests.test_e2e.conftest import (
    INDEX_KEYS,
    INDEX_NAME,
    TEST_ENTRY_NUM,
    TEST_INSERT_BATCH_SIZE,
)

logger = logging.getLogger(__name__)


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
            _batch_count = 0
            for _batch_count, entry in enumerate(
                batched(setup_test_data.values(), TEST_INSERT_BATCH_SIZE),
                start=1,
            ):
                pool.submit(thread_pool.orm_insert_entries, entry)

            logger.info(
                f"all insert tasks are dispatched: {_batch_count} batches with {TEST_INSERT_BATCH_SIZE=}"
            )

        logger.info("confirm data written")
        _selected_entry_count = 0
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
                SampleTableCols(
                    key_id=_entry.key_id,
                    prim_key_sha256hash=_entry.prim_key_sha256hash,
                ),
            )
            _looked_up = list(_looked_up)
            assert len(_looked_up) == 1
            assert _looked_up[0] == _entry

    def test_caller_exits_when_lookup_entries(
        self, thread_pool: SampleDBConnectionPool
    ):
        class _StopAt(Exception): ...

        def _wrapper():
            _stop_at = random.randrange(TEST_ENTRY_NUM // 2, TEST_ENTRY_NUM)

            _count = 0
            for _entry in thread_pool.orm_select_entries():
                _count += 1
                if _count >= _stop_at:
                    logger.info("break here!")
                    raise _StopAt("stop as expected")
                yield _entry

        with pytest.raises(_StopAt):
            for _ in _wrapper():
                ...
        logger.info("do breakout!")

    def test_in_thread_raise_exceptions_when_lookup_entries(
        self, thread_pool: SampleDBConnectionPool
    ):
        _origin_lookup_entries = SampleDB.orm_select_entries
        _stop_at = random.randrange(TEST_ENTRY_NUM // 2, TEST_ENTRY_NUM)

        class _StopAt(Exception): ...

        def _mocked_select_entries(*args, **kwargs):
            for _count, _entry in enumerate(_origin_lookup_entries(*args, **kwargs)):
                if _count >= _stop_at:
                    raise _StopAt("stop as expected")
                yield _entry

        # NOTE: bound the wrapped to thread_pool
        _wrapped_mocked_select_entries = _wrap_generator_with_thread_ctx(
            _mocked_select_entries
        ).__get__(thread_pool)

        with pytest.raises(_StopAt):
            for _ in _wrapped_mocked_select_entries():
                ...

    def test_delete_entries(
        self, thread_pool: SampleDBConnectionPool, entries_to_remove: list[SampleTable]
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
                _res = thread_pool.orm_delete_entries(
                    SampleTableCols(
                        key_id=entry.key_id,
                        prim_key_sha256hash=entry.prim_key_sha256hash,
                    ),
                    # _limit=1,
                )
                assert _res == 1
        else:
            for entry in entries_to_remove:
                _res = thread_pool.orm_delete_entries_with_returning(
                    SampleTableCols(
                        key_id=entry.key_id,
                        prim_key_sha256hash=entry.prim_key_sha256hash,
                    ),
                    _returning_cols="*",
                    # _limit=1,
                )
                _res_list = list(_res)

                assert len(_res_list) == 1
                assert _res_list[0] == entry


_THREADS = 6


class TestORMPoolShutdown:
    @pytest.fixture
    def _orm_pool(self, tmp_path: Path):
        return SampleDBConnectionPool(
            con_factory=partial(sqlite3.connect, tmp_path / "test.sqlite3"),
            number_of_cons=_THREADS,
        )

    def _workload(self, _id: int, event: threading.Event):
        logger.info(f"workload {_id} engaged by thread={threading.get_native_id()}")
        event.wait()
        time.sleep(random.random())
        if random.random() < 0.5:
            logger.info("raise")
            raise SystemExit

        logger.info(f"workload {_id} finished")

    def test_orm_pool_shutdown(self, _orm_pool: SampleDBConnectionPool):
        _event = threading.Event()

        # insert random workloads
        for _id in range(_THREADS * 10):
            _orm_pool._pool.submit(self._workload, _id, _event)
        _event.set()
        logger.info("workloads are all dispatched, now shutdown pool ...")

        _orm_pool.orm_pool_shutdown(wait=True, close_connections=True)
        assert not _orm_pool._thread_id_orms
