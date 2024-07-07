"""General utils for sqlite3."""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)

#
# ------ performance tuning ------ #
#
# ref: https://developer.android.com/topic/performance/sqlite-performance-best-practices
#       https://www.sqlite.org/pragma.html


def enable_wal_mode(con: sqlite3.Connection, relax_sync_mode: bool = True):
    """
    Note that for multiple databases being attached, WAL mode only guarantees
        atomic within each individual database file. See https://www.sqlite.org/lang_attach.html
        for more details.
    """
    with con as con:
        con.execute("PRAGMA journal_mode = WAL;")
        if relax_sync_mode:
            con.execute("PRAGMA synchronous = NORMAL;")


def enable_tmp_store_at_memory(con: sqlite3.Connection):
    """
    See https://www.sqlite.org/pragma.html#pragma_temp_store.
    """
    with con as con:
        con.execute("PRAGMA temp_store = MEMORY;")


DEFAULT_MMAP_SIZE = 16 * 1024 * 1024  # 16MiB


def enable_mmap(con: sqlite3.Connection, mmap_size: int = DEFAULT_MMAP_SIZE):
    """
    See https://www.sqlite.org/pragma.html#pragma_mmap_size.
    """
    with con as con:
        con.execute(f"PRAGMA mmap_size = {mmap_size};")


#
# ------ General DB operation ------ #
#


def check_db_integrity(con: sqlite3.Connection, table_name: str | None = None):
    """
    See https://www.sqlite.org/pragma.html#pragma_integrity_check.
    """
    query = "PRAGMA integrity_check;"
    if table_name:
        query = f"PRAGMA integrity_check({table_name});"

    with con as con:
        cur = con.execute(query)
        res = cur.fetchall()
        if len(res) == 1 and res[0] == ("ok",):
            return True
        logger.warning(f"database integrity check({query=}) finds problem: {res}")
        return False


def lookup_table(con: sqlite3.Connection, table_name: str) -> bool:
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    with con as con:
        cur = con.execute(query, (table_name,))
        return bool(cur.fetchone())
