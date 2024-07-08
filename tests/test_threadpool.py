from __future__ import annotations

import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Generator

import pytest

from simple_sqlite3_orm import utils
from simple_sqlite3_orm._orm import ORMConnectionThreadPool
from tests.sample_db.orm import SampleDB
from tests.sample_db.table import SampleTable
from tests.test_with_sample_db import INDEX_KEYS, INDEX_NAME, TABLE_NAME

THREAD_NUM = 3
CONNECTION_NUMBER = 4

logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def setup_connections(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[list[sqlite3.Connection], None, None]:
    tmp_path = tmp_path_factory.mktemp("tmp_db_path")
    db_file = tmp_path / "test_db_file.sqlite3"

    cons: list[sqlite3.Connection] = []

    for _ in range(CONNECTION_NUMBER):
        con = sqlite3.connect(db_file, check_same_thread=False)
        # enable optimization
        utils.enable_wal_mode(con, relax_sync_mode=True)
        utils.enable_mmap(con)
        utils.enable_tmp_store_at_memory(con)
        cons.append(con)

    try:
        yield cons
    finally:
        for con in cons:
            con.close()


class SampleDBConnectionPool(ORMConnectionThreadPool[SampleTable]):
    """Test connection pool."""


class TestWithSampleDBAndThreadPool:

    @pytest.fixture(autouse=True)
    def setup_test(
        self,
        setup_connections: list[sqlite3.Connection],
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

        self.cons = setup_connections[:-1]
        self.orm_inst = SampleDB(setup_connections[-1], TABLE_NAME)
        self.pool = SampleDBConnectionPool(TABLE_NAME, cons=self.cons)

    def test_create_table(self):
        logger.info("test create table")
        self.orm_inst.orm_create_table(without_rowid=True)

    def test_insert_entries_with_pool(self):
        # simulating multiple worker threads submitting to database
        logger.info("test insert entries...")
        with ThreadPoolExecutor(max_workers=THREAD_NUM) as pool:
            for entry in self.data_for_test.values():
                pool.submit(self.pool.orm_insert_entry, entry)

        logger.info("confirm data written")
        for _entry in self.pool.orm_select_entries(_return_as_generator=True):
            _corresponding_item = self.data_for_test[_entry.prim_key]
            assert _corresponding_item == _entry

    def test_create_index(self):
        logger.info("test create index")
        self.orm_inst.orm_create_index(
            index_name=INDEX_NAME,
            index_keys=INDEX_KEYS,
            unique=True,
        )
