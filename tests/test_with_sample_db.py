from __future__ import annotations

import random
import sqlite3
from typing import Iterator

import pytest

from tests.sample_db.table import SampleTable
from tests.sample_db.db import generate_testdata, SampleDB


class TestWithSampleDB:

    TEST_ENTRY_NUM = 4096
    TABLE_NAME = "test_table"

    @pytest.fixture(autouse=True, scope="class")
    def setup_test(self):
        self.data_for_test = generate_testdata(self.TEST_ENTRY_NUM)
        self.table_spec = SampleTable
        self.data_len = self.TEST_ENTRY_NUM
        try:
            with sqlite3.connect(":memory:") as con:
                self.orm_inst = SampleDB(con, table_name=self.TABLE_NAME)
                yield
        except Exception:
            pass

    def test_prepare_db(self):
        self.orm_inst.create_table(without_rowid=True)

    def test_create_index(self):
        _index_name = "key_id_prim_key_hash_idx"
        _cols = ["key_id", "prim_key_hash"]

        self.orm_inst.create_index(_index_name, *_cols, unique=True)

    def test_insert_entries(self):
        self.orm_inst.insert_entries(self.data_for_test.values())

        # confirm data written
        for _row in self.orm_inst.select_entries():
            _corresponding_item = self.data_for_test[_row.prim_key]
            assert _corresponding_item == _row

        # confirm the num of inserted entries
        with self.orm_inst.con as _con:
            _cur = _con.execute(
                self.table_spec.table_select_stmt(self.TABLE_NAME, function="count")
            )
            _raw = _cur.fetchone()
            assert _raw[0] == self.data_len

    def test_delete_entries(self):
        num_of_entries_to_remove = 3
        entries_to_be_removed = random.choices(
            list(self.data_for_test.values()),
            k=num_of_entries_to_remove,
        )

        # remove the entries and confirm the removed entry is the expected one
        for entry in entries_to_be_removed:
            _res = self.orm_inst.delete_entries(
                returning=True,
                key_id=entry.key_id,
                prim_key_hash=entry.prim_key_hash,
            )
            assert isinstance(_res, Iterator)
            assert next(_res) == entry
            assert next(_res, None) is None
            # also remove from the test date dict
            self.data_for_test.pop(entry.prim_key)

        # confirm the num of leftover entries in the db
        with self.orm_inst.con as _con:
            _cur = _con.execute(
                self.table_spec.table_select_stmt(self.TABLE_NAME, function="count")
            )
            _raw = _cur.fetchone()
            assert _raw[0] == self.data_len - num_of_entries_to_remove
        self.data_len -= num_of_entries_to_remove

    def test_clear_db(self):
        _returning = self.orm_inst.delete_entries(returning=True)
        assert isinstance(_returning, Iterator)
        # ensure the removed entries
        for _row in _returning:
            _corresponding_item = self.data_for_test[_row.prim_key]
            assert _corresponding_item == _row

        # ensure the database is empty now
        with self.orm_inst.con as _con:
            _cur = _con.execute(
                self.table_spec.table_select_stmt(self.TABLE_NAME, function="count")
            )
            _raw = _cur.fetchone()
            assert _raw[0] == 0
