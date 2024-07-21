from __future__ import annotations

import asyncio
import atexit
import logging
import queue
import sqlite3
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Generator,
    Generic,
    Iterable,
    Literal,
    TypeVar,
    overload,
)
from weakref import WeakValueDictionary

from typing_extensions import ParamSpec, deprecated

from simple_sqlite3_orm._sqlite_spec import INSERT_OR, ORDER_DIRECTION
from simple_sqlite3_orm._table_spec import TableSpec, TableSpecType

_parameterized_orm_cache: WeakValueDictionary[
    tuple[type[ORMBase], type[TableSpec]], type[ORMBase[Any]]
] = WeakValueDictionary()

logger = logging.getLogger(__name__)

P = ParamSpec("P")
RT = TypeVar("RT")

if sys.version_info >= (3, 9):
    from types import GenericAlias as _GenericAlias
else:
    from typing import List

    if not TYPE_CHECKING:
        _GenericAlias = type(List[int])
    else:

        class _GenericAlias(type(List)):
            def __new__(
                cls, _type: type[Any], _params: type[Any] | tuple[type[Any], ...]
            ):
                """For type check only, typing the _GenericAlias as GenericAlias."""


class ORMBase(Generic[TableSpecType]):
    """ORM layer for <TableSpecType>.

    NOTE that ORMBase will set the connection scope row_factory to <tablespec>'s table_row_factory.
        See TableSpec.table_row_factory for more details.

    NOTE that instance of ORMBase cannot be used in multi-threaded environment as the underlying
        sqlite3 connection. Use ORMThreadPoolBase for multi-threaded environment. For asyncio,
        use AsyncORMThreadPoolBase.

    The underlying connection can be used in multiple connection for accessing different table in
        the connected database.

    Attributes:
        con (sqlite3.Connection): The sqlite3 connection used by this ORM.
        table_name (str): The name of the table in the database <con> connected to.
        schema_name (str): the schema of the table if multiple databases are attached to <con>.
    """

    orm_table_spec: type[TableSpecType]

    def __init__(
        self,
        con: sqlite3.Connection,
        table_name: str,
        schema_name: str | Literal["temp"] | None = None,
    ) -> None:
        self._table_name = table_name
        self._schema_name = schema_name
        self._con = con
        con.row_factory = self.orm_table_spec.table_row_factory

    def __class_getitem__(cls, params: Any | type[Any] | type[TableSpecType]) -> Any:
        # just for convienience, passthrough anything that is not type[TableSpecType]
        #   to Generic's __class_getitem__ and return it.
        # Typically this is for subscript ORMBase with TypeVar or another Generic.
        if not (isinstance(params, type) and issubclass(params, TableSpec)):
            return super().__class_getitem__(params)  # type: ignore

        key = (cls, params)
        if _cached_type := _parameterized_orm_cache.get(key):
            return _GenericAlias(_cached_type, params)

        new_parameterized_ormbase: type[ORMBase] = type(
            f"{cls.__name__}[{params.__name__}]", (cls,), {}
        )
        new_parameterized_ormbase.orm_table_spec = params  # type: ignore
        _parameterized_orm_cache[key] = new_parameterized_ormbase
        return _GenericAlias(new_parameterized_ormbase, params)

    @property
    def orm_con(self) -> sqlite3.Connection:
        """A reference to the underlying sqlite3.Connection.

        This is for advanced database execution.
        """
        return self._con

    @cached_property
    def orm_table_name(self) -> str:
        """The unique name of the table for use in sql statement.

        If multiple databases are attached to <con> and <schema_name> is availabe,
            return "<schema_name>.<table_name>", otherwise return <table_name>.
        """
        return (
            f"{self._schema_name}.{self._table_name}"
            if self._schema_name
            else self._table_name
        )

    def orm_execute(
        self, sql_stmt: str, params: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> list[Any]:
        """Execute one sql statement and get the all the result.

        The result will be fetched with fetchall API and returned as it.

        This method is inteneded for executing simple sql_stmt with small result.
        For complicated sql statement and large result, please use sqlite3.Connection object
            exposed by orm_con and manipulate the Cursor object by yourselves.

        Args:
            sql_stmt (str): The sqlite statement to be executed.
            params (tuple[Any, ...] | dict[str, Any] | None, optional): The parameters to be bound
                to the sql statement execution. Defaults to None, not passing any params.

        Returns:
            list[Any]: A list contains all the result entries.
        """
        with self._con as con:
            if params:
                cur = con.execute(sql_stmt, params)
            else:
                cur = con.execute(sql_stmt)
            return cur.fetchall()

    def orm_create_table(
        self,
        *,
        allow_existed: bool = False,
        strict: bool = False,
        without_rowid: bool = False,
    ) -> None:
        """Create the table defined by this ORM with <orm_table_spec>.

        NOTE: strict table option is supported after sqlite3 3.37.

        Args:
            allow_existed (bool, optional): Do not abort on table already created.
                Set True equals to add "IF NOT EXISTS" in the sql statement. Defaults to False.
            strict (bool, optional): Enable strict field type check. Defaults to False.
                See https://www.sqlite.org/stricttables.html for more details.
            without_rowid (bool, optional): Create the table without ROWID. Defaults to False.
                See https://www.sqlite.org/withoutrowid.html for more details.

        Raises:
            sqlite3.DatabaseError on failed sql execution.
        """
        with self._con as con:
            con.execute(
                self.orm_table_spec.table_create_stmt(
                    self.orm_table_name,
                    if_not_exists=allow_existed,
                    strict=strict,
                    without_rowid=without_rowid,
                )
            )

    def orm_create_index(
        self,
        *,
        index_name: str,
        index_keys: tuple[str, ...],
        allow_existed: bool = False,
        unique: bool = False,
    ) -> None:
        """Create index according to the input arguments.

        Args:
            index_name (str): The name of the index.
            index_keys (tuple[str, ...]): The columns for the index.
            allow_existed (bool, optional): Not abort on index already created. Defaults to False.
            unique (bool, optional): Not allow duplicated entries in the index. Defaults to False.

        Raises:
            sqlite3.DatabaseError on failed sql execution.
        """
        index_create_stmt = self.orm_table_spec.table_create_index_stmt(
            table_name=self.orm_table_name,
            index_name=index_name,
            unique=unique,
            if_not_exists=allow_existed,
            index_cols=index_keys,
        )
        with self._con as con:
            con.execute(index_create_stmt)

    def orm_select_entries(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION], ...] | None = None,
        _limit: int | None = None,
        **col_values: Any,
    ) -> Generator[TableSpecType, None, None]:
        """Select entries from the table accordingly.

        Args:
            _distinct (bool, optional): Deduplicate and only return unique entries. Defaults to False.
            _order_by (tuple[str  |  tuple[str, ORDER_DIRECTION], ...] | None, optional):
                Order the result accordingly. Defaults to None, not sorting the result.
            _limit (int | None, optional): Limit the number of result entries. Defaults to None.

        Raises:
            sqlite3.DatabaseError on failed sql execution.

        Yields:
            Generator[TableSpecType, None, None]: A generator that can be used to yield entry from result.
        """
        table_select_stmt = self.orm_table_spec.table_select_stmt(
            select_from=self.orm_table_name,
            distinct=_distinct,
            order_by=_order_by,
            limit=_limit,
            where_cols=tuple(col_values),
        )

        with self._con as con:
            _cur = con.execute(table_select_stmt, col_values)
            yield from _cur

    def orm_select_entry(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION], ...] | None = None,
        **col_values: Any,
    ) -> TableSpecType | None:
        """Select exactly one entry from the table accordingly.

        NOTE that if the select result contains more than one entry, this method will return
            the FIRST one from the result with fetchone API.

        Args:
            _distinct (bool, optional): Deduplicate and only return unique entries. Defaults to False.
            _order_by (tuple[str  |  tuple[str, ORDER_DIRECTION], ...] | None, optional):
                Order the result accordingly. Defaults to None, not sorting the result.

        Raises:
            sqlite3.DatabaseError on failed sql execution.

        Returns:
            Exactly one <TableSpecType> entry, or None if not hit.
        """
        table_select_stmt = self.orm_table_spec.table_select_stmt(
            select_from=self.orm_table_name,
            distinct=_distinct,
            order_by=_order_by,
            where_cols=tuple(col_values),
        )

        with self._con as con:
            _cur = con.execute(table_select_stmt, col_values)
            return _cur.fetchone()

    def orm_insert_entries(
        self, _in: Iterable[TableSpecType], *, or_option: INSERT_OR | None = None
    ) -> int:
        """Insert entry/entries into this table.

        Args:
            _in (Iterable[TableSpecType]): A list of entries to insert.

        Raises:
            ValueError: On invalid types of _in.
            sqlite3.DatabaseError: On failed sql execution.

        Returns:
            int: Number of inserted entries.
        """
        insert_stmt = self.orm_table_spec.table_insert_stmt(
            insert_into=self.orm_table_name,
            or_option=or_option,
        )
        with self._con as con:
            _cur = con.executemany(
                insert_stmt, (_row.table_dump_asdict() for _row in _in)
            )
            return _cur.rowcount

    def orm_insert_entry(
        self, _in: TableSpecType, *, or_option: INSERT_OR | None = None
    ) -> int:
        """Insert exactly one entry into this table.

        Args:
            _in (TableSpecType): The instance of entry to insert.

        Raises:
            ValueError: On invalid types of _in.
            sqlite3.DatabaseError: On failed sql execution.

        Returns:
            int: Number of inserted entries. In normal case it should be 1.
        """
        insert_stmt = self.orm_table_spec.table_insert_stmt(
            insert_into=self.orm_table_name,
            or_option=or_option,
        )
        with self._con as con:
            _cur = con.execute(insert_stmt, _in.table_dump_asdict())
            return _cur.rowcount

    @overload
    def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION]] | None = None,
        _limit: int | None = None,
        _returning_cols: None = None,
        **cols_value: Any,
    ) -> int: ...

    @overload
    def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION]] | None = None,
        _limit: int | None = None,
        _returning_cols: tuple[str, ...] | Literal["*"],
        **cols_value: Any,
    ) -> Generator[TableSpecType, None, None]: ...

    def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION]] | None = None,
        _limit: int | None = None,
        _returning_cols: tuple[str, ...] | Literal["*"] | None = None,
        **cols_value: Any,
    ) -> int | Generator[TableSpecType, None, None]:
        """Delete entries from the table accordingly.

        Args:
            _order_by (tuple[str  |  tuple[str, ORDER_DIRECTION]] | None, optional): Order the matching entries
                before executing the deletion, used together with <_limit>. Defaults to None.
            _limit (int | None, optional): Only delete <_limit> number of entries. Defaults to None.
            _returning_cols (tuple[str, ...] | Literal[, optional): Return the deleted entries on execution.
                NOTE that only sqlite3 version >= 3.35 supports returning statement. Defaults to None.

        Returns:
            int: The num of entries deleted.
            Generator[TableSpecType, None, None]: If <_returning_cols> is defined, returns a generator which can
                be used to yield the deleted entries from.
        """
        delete_stmt = self.orm_table_spec.table_delete_stmt(
            delete_from=self.orm_table_name,
            limit=_limit,
            order_by=_order_by,
            returning_cols=_returning_cols,
            where_cols=tuple(cols_value),
        )

        if _returning_cols:

            def _gen():
                with self._con as con:
                    _cur = con.execute(delete_stmt, cols_value)
                    yield from _cur

            return _gen()

        else:
            with self._con as con:
                _cur = con.execute(delete_stmt, cols_value)
                return _cur.rowcount


ORMBaseType = TypeVar("ORMBaseType", bound=ORMBase)

_global_shutdown = False


def _python_exit():
    global _global_shutdown
    _global_shutdown = True


atexit.register(_python_exit)


class ORMThreadPoolBase(ORMBase[TableSpecType]):
    """
    See https://www.sqlite.org/wal.html#concurrency for more details.
    """

    def __init__(
        self,
        table_name: str,
        schema_name: str | None = None,
        *,
        con_factory: Callable[[], sqlite3.Connection],
        number_of_cons: int,
        thread_name_prefix: str = "",
    ) -> None:
        self._table_name = table_name
        self._schema_name = schema_name

        self._thread_id_cons: dict[int, sqlite3.Connection] = {}

        def _thread_initializer():
            thread_id = threading.get_native_id()
            self._thread_id_cons[thread_id] = con = con_factory()
            con.row_factory = self.orm_table_spec.table_row_factory

        self._pool = ThreadPoolExecutor(
            max_workers=number_of_cons,
            initializer=_thread_initializer,
            thread_name_prefix=thread_name_prefix,
        )

    @property
    def _con(self) -> sqlite3.Connection:
        """Get thread-specific sqlite3 connection."""
        return self._thread_id_cons[threading.get_native_id()]

    @property
    @deprecated("orm_con is not available in thread pool ORM")
    def orm_con(self):
        """Not implemented, orm_con is not available in thread pool ORM."""
        raise NotImplementedError("orm_con is not available in thread pool ORM")

    def orm_pool_shutdown(self, *, wait=True, close_connections=True) -> None:
        """Shutdown the ORM connections thread pool.

        It is safe to call this method multiple time.
        This method is NOT thread-safe, and should be called at the main thread,
            or the thread that creates this thread pool.

        Args:
            wait (bool, optional): Wait for threads join. Defaults to True.
            close_connections (bool, optional): Close all the connections. Defaults to True.
        """
        self._pool.shutdown(wait=wait)
        if close_connections:
            for con in self._thread_id_cons.values():
                con.close()
        self._thread_id_cons = {}

    def orm_execute(
        self, sql_stmt: str, params: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> Future[list[Any]]:
        return self._pool.submit(super().orm_execute, sql_stmt, params)

    orm_execute.__doc__ = ORMBase.orm_execute.__doc__

    def orm_create_table(
        self,
        *,
        allow_existed: bool = False,
        strict: bool = False,
        without_rowid: bool = False,
    ) -> Future[None]:
        return self._pool.submit(
            super().orm_create_table,
            allow_existed=allow_existed,
            strict=strict,
            without_rowid=without_rowid,
        )

    orm_create_table.__doc__ = ORMBase.orm_create_table.__doc__

    def orm_create_index(
        self,
        *,
        index_name: str,
        index_keys: tuple[str, ...],
        allow_existed: bool = False,
        unique: bool = False,
    ) -> Future[None]:
        return self._pool.submit(
            super().orm_create_index,
            index_name=index_name,
            index_keys=index_keys,
            allow_existed=allow_existed,
            unique=unique,
        )

    orm_create_index.__doc__ = ORMBase.orm_create_index.__doc__

    def orm_select_entries_gen(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]], ...] | None = None,
        _limit: int | None = None,
        **col_values: Any,
    ) -> Generator[TableSpecType, None, None]:
        """Select multiple entries and return a generator for yielding entries from."""
        _queue = queue.SimpleQueue()

        def _inner():
            global _global_shutdown
            try:
                for entry in ORMBase.orm_select_entries(
                    self,
                    _distinct=_distinct,
                    _order_by=_order_by,
                    _limit=_limit,
                    **col_values,
                ):
                    if _global_shutdown:
                        break
                    _queue.put_nowait(entry)
            except Exception as e:
                _queue.put_nowait(e)
            finally:
                _queue.put_nowait(None)

        self._pool.submit(_inner)

        def _gen():
            while entry := _queue.get():
                if isinstance(entry, Exception):
                    try:
                        raise entry from None
                    finally:
                        del entry
                yield entry

        return _gen()

    def orm_select_entries(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]], ...] | None = None,
        _limit: int | None = None,
        **col_values: Any,
    ) -> Future[list[TableSpecType]]:
        """Select multiple entries and return all the entries in a list."""

        def _inner():
            return list(
                ORMBase.orm_select_entries(
                    self,
                    _distinct=_distinct,
                    _order_by=_order_by,
                    _limit=_limit,
                    **col_values,
                )
            )

        return self._pool.submit(_inner)

    def orm_insert_entries(
        self, _in: Iterable[TableSpecType], *, or_option: INSERT_OR | None = None
    ) -> Future[int]:
        return self._pool.submit(super().orm_insert_entries, _in, or_option=or_option)

    orm_insert_entries.__doc__ = ORMBase.orm_insert_entries.__doc__

    def orm_insert_entry(
        self, _in: TableSpecType, *, or_option: INSERT_OR | None = None
    ) -> Future[int]:
        return self._pool.submit(super().orm_insert_entry, _in, or_option=or_option)

    orm_insert_entry.__doc__ = ORMBase.orm_insert_entry.__doc__

    def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]]] | None = None,
        _limit: int | None = None,
        _returning_cols: tuple[str, ...] | None | Literal["*"] = None,
        **cols_value: Any,
    ) -> Future[int | list[TableSpecType]]:
        # NOTE(20240708): currently we don't support generator for delete with RETURNING statement
        def _inner():
            res = ORMBase.orm_delete_entries(
                self,
                _order_by=_order_by,
                _limit=_limit,
                _returning_cols=_returning_cols,
                **cols_value,
            )

            if isinstance(res, int):
                return res
            return list(res)

        return self._pool.submit(_inner)

    orm_delete_entries.__doc__ = ORMBase.orm_delete_entries.__doc__


class AsyncORMThreadPoolBase(ORMThreadPoolBase[TableSpecType]):

    def __init__(
        self,
        table_name: str,
        schema_name: str | None = None,
        *,
        con_factory: Callable[[], sqlite3.Connection],
        number_of_cons: int,
        thread_name_prefix: str = "",
    ) -> None:
        # setup the thread pool
        super().__init__(
            table_name,
            schema_name,
            con_factory=con_factory,
            number_of_cons=number_of_cons,
            thread_name_prefix=thread_name_prefix,
        )

        self._loop = asyncio.get_running_loop()

    def _run_in_pool(
        self, func: Callable[P, RT], *args: P.args, **kwargs: P.kwargs
    ) -> asyncio.Future[RT]:
        """Run normal function in threadpool and track the result async."""
        return asyncio.wrap_future(
            self._pool.submit(func, *args, **kwargs),
            loop=self._loop,
        )

    @property
    @deprecated("orm_con is not available in thread pool ORM")
    def orm_con(self):
        """Not implemented, orm_con is not available in thread pool ORM."""
        raise NotImplementedError("orm_con is not available in thread pool ORM")

    async def orm_execute(
        self, sql_stmt: str, params: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> list[Any]:
        return await self._run_in_pool(ORMBase.orm_execute, self, sql_stmt, params)

    orm_execute.__doc__ = ORMBase.orm_execute.__doc__

    def orm_select_entries_gen(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]], ...] | None = None,
        _limit: int | None = None,
        **col_values: Any,
    ) -> AsyncGenerator[TableSpecType, Any]:
        """Select multiple entries and return an async generator for yielding entries from."""
        _async_queue = asyncio.Queue()

        def _inner():
            global _global_shutdown
            try:
                for entry in ORMBase.orm_select_entries(
                    self,
                    _distinct=_distinct,
                    _order_by=_order_by,
                    _limit=_limit,
                    **col_values,
                ):
                    if _global_shutdown:
                        break
                    self._loop.call_soon_threadsafe(_async_queue.put_nowait, entry)
            except Exception as e:
                self._loop.call_soon_threadsafe(_async_queue.put_nowait, e)
            finally:
                self._loop.call_soon_threadsafe(_async_queue.put_nowait, None)

        self._pool.submit(_inner)

        async def _gen():
            while entry := await _async_queue.get():
                if isinstance(entry, Exception):
                    try:
                        raise entry from None
                    finally:
                        del entry
                yield entry

        return _gen()

    async def orm_select_entries(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]], ...] | None = None,
        _limit: int | None = None,
        **col_values: Any,
    ) -> list[TableSpecType]:
        """Select multiple entries and return all the entries in a list."""

        def _inner():
            return list(
                ORMBase.orm_select_entries(
                    self,
                    _distinct=_distinct,
                    _order_by=_order_by,
                    _limit=_limit,
                    **col_values,
                )
            )

        return await self._run_in_pool(_inner)

    async def orm_select_entry(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]], ...] | None = None,
        **col_values: Any,
    ) -> TableSpecType | None:
        return await self._run_in_pool(
            ORMBase.orm_select_entry,
            self,
            _distinct=_distinct,
            _order_by=_order_by,
            **col_values,
        )

    orm_select_entry.__doc__ = ORMBase.orm_select_entry

    async def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]]] | None = None,
        _limit: int | None = None,
        _returning_cols: tuple[str, ...] | None | Literal["*"] = None,
        **cols_value: Any,
    ) -> list[TableSpecType] | int:
        # NOTE(20240708): currently we don't support async generator for delete with RETURNING statement
        def _inner():
            res = ORMBase.orm_delete_entries(
                self,
                _order_by=_order_by,
                _limit=_limit,
                _returning_cols=_returning_cols,
                **cols_value,
            )

            if isinstance(res, int):
                return res
            return list(res)

        return await self._run_in_pool(_inner)

    orm_delete_entries.__doc__ = ORMBase.orm_delete_entries.__doc__

    async def orm_create_table(
        self,
        *,
        allow_existed: bool = False,
        strict: bool = False,
        without_rowid: bool = False,
    ) -> None:
        return await self._run_in_pool(
            ORMBase.orm_create_table,
            self,
            allow_existed=allow_existed,
            strict=strict,
            without_rowid=without_rowid,
        )

    orm_create_table.__doc__ = ORMBase.orm_create_table.__doc__

    async def orm_create_index(
        self,
        *,
        index_name: str,
        index_keys: tuple[str, ...],
        allow_existed: bool = False,
        unique: bool = False,
    ) -> None:
        return await self._run_in_pool(
            ORMBase.orm_create_index,
            self,
            index_name=index_name,
            index_keys=index_keys,
            allow_existed=allow_existed,
            unique=unique,
        )

    orm_create_index.__doc__ = ORMBase.orm_create_index.__doc__

    async def orm_insert_entries(
        self, _in: Iterable[TableSpecType], *, or_option: INSERT_OR | None = None
    ) -> int:
        return await self._run_in_pool(
            ORMBase.orm_insert_entries, self, _in, or_option=or_option
        )

    orm_insert_entries.__doc__ = ORMBase.orm_insert_entries.__doc__

    async def orm_insert_entry(
        self, _in: TableSpecType, *, or_option: INSERT_OR | None = None
    ) -> int:
        return await self._run_in_pool(
            ORMBase.orm_insert_entry, self, _in, or_option=or_option
        )

    orm_insert_entry.__doc__ = ORMBase.orm_insert_entry.__doc__
