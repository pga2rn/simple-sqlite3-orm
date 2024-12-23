"""General utils for sqlite3."""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
from enum import Enum
from io import StringIO
from itertools import islice
from typing import (
    TYPE_CHECKING,
    Any,
    Generator,
    Iterable,
    Literal,
    get_args,
    get_origin,
    overload,
)

from simple_sqlite3_orm._sqlite_spec import COMPARE_OPERATORS, CONDITION_OPERATORS

if TYPE_CHECKING:
    from simple_sqlite3_orm._orm import ORMBase
    from simple_sqlite3_orm._table_spec import TableSpecType

logger = logging.getLogger(__name__)

#
# ------ performance tuning ------ #
#
# ref: https://developer.android.com/topic/performance/sqlite-performance-best-practices
#       https://www.sqlite.org/pragma.html


def enable_wal_mode(con: sqlite3.Connection, relax_sync_mode: bool = True):
    """Enable WAL mode for the connected database.

    Note that for multiple databases being attached, WAL mode only guarantees
        atomic within each individual database file. See https://www.sqlite.org/lang_attach.html
        for more details.

    Args:
        con (sqlite3.Connection): The connection to the target database.
        relax_sync_mode (bool): Also set the synchronous mode to NORMAL. Default to True.

    Raises:
        sqlite3.DatabaseError on failed sql execution.
    """
    with con as con:
        con.execute("PRAGMA journal_mode = WAL;")
        if relax_sync_mode:
            con.execute("PRAGMA synchronous = NORMAL;")


def enable_tmp_store_at_memory(con: sqlite3.Connection):
    """Locate the temp tables at memory.

    See https://www.sqlite.org/pragma.html#pragma_temp_store for more details.

    Args:
        con (sqlite3.Connection): The connection to the target database.

    Raises:
        sqlite3.DatabaseError on failed sql execution.
    """
    with con as con:
        con.execute("PRAGMA temp_store = MEMORY;")


DEFAULT_MMAP_SIZE = 16 * 1024 * 1024  # 16MiB


def enable_mmap(con: sqlite3.Connection, mmap_size: int = DEFAULT_MMAP_SIZE):
    """Enable mmap for the connection.

    See https://www.sqlite.org/pragma.html#pragma_mmap_size for more

    Args:
        con (sqlite3.Connection): The connection to the target database.
        mmap_size (int, optional): The max mmap size. Defaults to <DEFAULT_MMAP_SIZE=16MiB>.

    Raises:
        sqlite3.DatabaseError on failed sql execution.
    """
    with con as con:
        con.execute(f"PRAGMA mmap_size = {mmap_size};")


def optimize_db(con: sqlite3.Connection):
    """Execute optimize PRAGMA on the target database.

    See https://www.sqlite.org/pragma.html#pragma_optimize.

    Args:
        con (sqlite3.Connection): The connection to the target database.

    Raises:
        sqlite3.DatabaseError on failed sql execution.
    """
    with con as con:
        con.execute("PRAGMA optimize;")


#
# ------ General DB operation ------ #
#


def check_db_integrity(con: sqlite3.Connection, table_name: str | None = None) -> bool:
    """Execute integrity_check PRAGMA on the target database(or specific table at the database).

    See https://www.sqlite.org/pragma.html#pragma_integrity_check for more details.

    Args:
        con (sqlite3.Connection): The connection to the target database.
        table_name (str | None, optional): If specified, the integrity_check will only be performed
            against this table. Defaults to None, means performing the check on the whole database.

    Returns:
        bool: True for integrity_check passed on the target database, False for errors found.
    """
    with con as con:
        if table_name:
            cur = con.execute("PRAGMA integrity_check(?);", (table_name,))
        else:
            cur = con.execute("PRAGMA integrity_check;")

        res = cur.fetchall()
        if len(res) == 1 and res[0] == ("ok",):
            return True
        logger.warning(f"database integrity check finds problem: {res}")
        return False


def lookup_table(con: sqlite3.Connection, table_name: str) -> bool:
    """Check if specific table existed on the target database.

    Args:
        con (sqlite3.Connection): The connection to the target database.
        table_name (str): The name of table to lookup.

    Returns:
        bool: True for table existed, False for not found.
    """
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    with con as con:
        cur = con.execute(query, (table_name,))
        return bool(cur.fetchone())


def attach_database(
    con: sqlite3.Connection, database: str | Literal[":memory:"], schema_name: str
) -> str:
    """Attach another database onto the current connection.

    See https://www.sqlite.org/lang_attach.html for more details.

    Args:
        con (sqlite3.Connection): The current database connection.
        database (str | Literal[":memory:"]): The new database to be attached.
        schema_name (str): The alias name of the newly connected database for distinguishing
            between the already connected database in this connection.

    Returns:
        str: The schema_name of the newly connected database in the connection.
    """
    query = "ATTACH DATABASE ? AS ?"
    with con as con:
        con.execute(query, (database, schema_name))
    return schema_name


FLAG_OPTION = object()


@overload
def check_pragma_compile_time_options(
    con: sqlite3.Connection, option_name: str
) -> tuple[str, Any] | None: ...


@overload
def check_pragma_compile_time_options(
    con: sqlite3.Connection, option_name: None = None
) -> list[tuple[str, Any]]: ...


def check_pragma_compile_time_options(
    con: sqlite3.Connection, option_name: str | None = None
) -> tuple[str, Any] | None | list[tuple[str, Any]]:
    """Get the runtime sqlite3 library's compile time options.

    Args:
        con (sqlite3.Connection): The current database connection.
        option_name (str | None, optional): The option to lookup. If not specified,
            it will return all the options and its values. Defaults to None.

    Returns:
        tuple[str, Any] | None | list[tuple[str, Any]]: Looked up options and its values.
    """
    query = "SELECT * FROM pragma_compile_options"
    with con as con:
        cur = con.execute(query)
        all_res: list[tuple[str]] = cur.fetchall()

        res: list[tuple[str, Any]] = []
        for raw_option in all_res:
            splitted = raw_option[0].split("=", maxsplit=1)
            _op_name, _op_value, *_ = *splitted, FLAG_OPTION

            if option_name == _op_name:
                return _op_name, _op_value
            res.append((_op_name, _op_value))

        if option_name:
            return
        return res


#
# ------ other tools ------ #
#

if sys.version_info >= (3, 12):
    from itertools import batched

else:

    def batched(
        iterable: Iterable[Any], n: int
    ) -> Generator[tuple[Any, ...], Any, None]:
        """Batch data from the iterable into tuples of length n. The last batch may be shorter than n.

        Backport batched from py3.12. This is the roughly python implementation
            of py3.12's batched copied from py3.12 documentation.
        See https://docs.python.org/3/library/itertools.html#itertools.batched for more details.

        Args:
            iterable (Iterable[Any]): The input to be batched.
            n (int): the size of each batch.

        Raises:
            ValueError on invalid n.

        Returns:
            A generator that can be used to loop over the input iterable and accumulates data into
                tuples up to size n(a.k.a, batch in size of n).
        """
        if n < 1:
            raise ValueError("n must be at least one")
        iterator = iter(iterable)
        while batch := tuple(islice(iterator, n)):
            yield batch


def gen_check_constrain(_in: Any, field_name: str) -> str:
    """Generate the constrain statement for CHECK keyword.

    Supports the following types:
    1. StrEnum or IntEnum types: will generate statement like:
        <field_name> IN (<enum_value_1>[, <enum_value_2>[, ...]])
    2. Literal types: similar to StrEnum and IntEnum.

    Args:
        enum_type (type[Enum]): The enum type to generate CHECK statement against.
        field_name (str): The field name of this enum_type in use.

    Raises:
        TypeError on unsupported enum_type.

    Returns:
        str: the generated statement can be used with CHECK keyword like the following:
           <enum_value_1>[, <enum_value_2>[, ...]]
    """
    if (_origin := get_origin(_in)) and _origin is Literal:
        values = (wrap_value(v) for v in get_args(_in))
        return f"{field_name} IN ({','.join(values)})"
    if not isinstance(_in, type):
        raise TypeError("expect Literal or types")
    if issubclass(_in, Enum):
        enum_values = (wrap_value(e.value) for e in _in)
        return f"{field_name} IN ({','.join(enum_values)})"
    raise TypeError(f"expect StrEnum, IntEnum or Literal, get {type(_in)}")


def concatenate_condition(
    *condition_or_op: CONDITION_OPERATORS | COMPARE_OPERATORS | Any,
    wrapped_with_parentheses: bool = True,
) -> str:
    """Chain a list of conditions and operators together in a string.

    For example, for the following statement for CHECK keyword:
        (column IS NULL OR column IN (1, 2, 3))
    we can use concatenate_condition like:
        concatenate_condition(
            "column IS NULL", "OR", "column IN (1, 2, 3)",
            wrapped_with_parentheses=True,
        )
    """
    res = " ".join(condition_or_op)
    if wrapped_with_parentheses:
        res = f"({res})"
    return res


def wrap_value(value: Any) -> str:
    """Wrap value for use in sql statement.

    NOTE that for most cases, you should use python sqlite3 lib's
        placeholder feature to bind value in the sql statement.

    For int and float, the value will be used as it.
    For str, the value will be wrapped with parenthesis.
    For bytes, the value will be converted as x'<bytes_in_hex>'.
    """
    # NOTE: handle Enum with data type first
    if isinstance(value, int) and isinstance(value, Enum):
        return f"{value.value}"
    if isinstance(value, str) and isinstance(value, Enum):
        return rf'"{value.value}"'

    if isinstance(value, (int, float)):
        return f"{value}"
    if isinstance(value, str):
        return rf'"{value}"'
    if isinstance(value, bytes):
        return rf"x'{value.hex()}'"
    if value is None:
        return "NULL"
    raise TypeError("only accept int, float, str, None or bytes")


def gen_sql_script(*stmts: str) -> str:
    """Combine multiple sql statements into a sql script."""
    with StringIO() as buffer:
        for stmt in stmts:
            if not stmt:
                continue
            buffer.write(" ")
            buffer.write(stmt.strip(";"))
            buffer.write(";")
        return buffer.getvalue().strip()


#
# ------ advanced helper tools ------ #
#


def sort_and_replace(
    _orm: ORMBase[TableSpecType], table_name: str, *, order_by_col: str
) -> None:
    """Sort the table, and then replace the old table with the sorted one."""
    _original_table_name = table_name
    _sorted_table_name = f"{table_name}_sorted_{os.urandom(2).hex()}"
    _table_spec = _orm.orm_table_spec

    _table_create_stmt = _table_spec.table_create_stmt(_sorted_table_name)
    _table_select_stmt = _table_spec.table_select_stmt(
        select_from=_original_table_name, order_by=(order_by_col,)
    )
    _dump_sorted = f"INSERT INTO {_sorted_table_name} {_table_select_stmt}"

    conn = _orm.orm_con
    with conn as conn:
        conn.executescript(
            gen_sql_script(
                "BEGIN;",
                _table_create_stmt,
                _dump_sorted,
                f"DROP TABLE {_original_table_name};",
                f"ALTER TABLE {_sorted_table_name} RENAME TO {_original_table_name};",
            )
        )
    with conn as conn:
        conn.execute("VACUUM;")
