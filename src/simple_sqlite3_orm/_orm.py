from __future__ import annotations

import asyncio
import atexit
import logging
import queue
import sqlite3
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, partial
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

from typing_extensions import ParamSpec

from simple_sqlite3_orm._sqlite_spec import ORDER_DIRECTION
from simple_sqlite3_orm._table_spec import TableSpec, TableSpecType
from simple_sqlite3_orm._typing import copy_callable_typehint

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
    """ORM for <TableSpecType>.

    NOTE that ORMBase will set the connection scope row_factory to <tablespec>'s table_row_factory.

    Attributes:
        con: the sqlite3 connection used by this ORM.
        table_name: the name of the table in the database <con> connected to.
        schema_name: the schema of the table if multiple databases are attached to <con>.
    """

    orm_table_spec: type[TableSpecType]

    def __init__(
        self,
        con: sqlite3.Connection,
        table_name: str,
        schema_name: str | None = None,
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
        """The unique name of the table.

        If multiple databases are attached to <con> and <schema_name> is availabe,
            return "<schema_name>.<table_name>", otherwise return <table_name>.
        """
        return (
            f"{self._schema_name}.{self._table_name}"
            if self._schema_name
            else self._table_name
        )

    def orm_create_table(
        self,
        *,
        allow_existed: bool = False,
        without_rowid: bool = False,
    ) -> None:
        with self._con as con:
            con.execute(
                self.orm_table_spec.table_create_stmt(
                    self.orm_table_name,
                    if_not_exists=allow_existed,
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

    def orm_insert_entries(self, _in: Iterable[TableSpecType]) -> int:
        """Insert entry/entries into this table.

        Args:
            _in (Iterable[TableSpecType]): A list of entries to insert.

        Raises:
            ValueError: On invalid types of _in.

        Returns:
            int: Number of inserted entries.
        """
        insert_stmt = self.orm_table_spec.table_insert_stmt(
            insert_into=self.orm_table_name
        )
        with self._con as con:
            _cur = con.executemany(
                insert_stmt, (_row.table_dump_asdict() for _row in _in)
            )
            return _cur.rowcount

    def orm_insert_entry(self, _in: TableSpecType) -> int:
        """Insert exactly one entry into this table.

        Args:
            _in (TableSpecType): The instance of entry to insert.

        Returns:
            int: Number of inserted entries.
        """
        insert_stmt = self.orm_table_spec.table_insert_stmt(
            insert_into=self.orm_table_name
        )
        with self._con as con:
            _cur = con.execute(insert_stmt, _in.table_dump_asdict())
            return _cur.rowcount

    def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION]] | None = None,
        _limit: int | None = None,
        _returning_cols: tuple[str, ...] | Literal["*"] | None = None,
        **cols_value: Any,
    ) -> int | Generator[TableSpecType, None, None]:
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
                _cur = con.execute(delete_stmt, tuple(cols_value.values()))
                return _cur.rowcount


ORMBaseType = TypeVar("ORMBaseType", bound=ORMBase)

_global_shutdown = False


def _python_exit():
    global _global_shutdown
    _global_shutdown = True


atexit.register(_python_exit)


class ORMConnectionThreadPool(ORMBase[TableSpecType]):
    """
    See https://www.sqlite.org/wal.html#concurrency for more details.
    """

    @property
    def _con(self) -> sqlite3.Connection:
        """Get thread-specific sqlite3 connection."""
        return self._thread_id_cons[threading.get_native_id()]

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

        self._thread_id_cons = thread_cons_map = {}

        def _thread_initializer():
            thread_id = threading.get_native_id()
            thread_cons_map[thread_id] = con = con_factory()
            con.row_factory = self.orm_table_spec.table_row_factory

        self._pool = ThreadPoolExecutor(
            max_workers=number_of_cons,
            initializer=_thread_initializer,
            thread_name_prefix=thread_name_prefix,
        )

    def orm_pool_shutdown(self, *, wait=True):
        self._pool.shutdown(wait=wait)
        for con in self._thread_id_cons.values():
            con.close()
        self._thread_id_cons = {}

    @copy_callable_typehint(ORMBase.orm_create_table)
    def orm_create_table(self, *args, **kwargs) -> None:
        self._pool.submit(super().orm_create_table, *args, **kwargs).result()

    @copy_callable_typehint(ORMBase.orm_create_index)
    def orm_create_index(self, *args, **kwargs) -> None:
        self._pool.submit(super().orm_create_index, *args, **kwargs).result()

    @overload
    def orm_select_entries(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]], ...] | None = None,
        _limit: int | None = None,
        _return_as_generator: bool = False,
        **col_values: Any,
    ) -> list[TableSpecType]: ...

    @overload
    def orm_select_entries(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]], ...] | None = None,
        _limit: int | None = None,
        _return_as_generator: bool = True,
        **col_values: Any,
    ) -> Generator[TableSpecType, None, None]: ...

    def orm_select_entries(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]], ...] | None = None,
        _limit: int | None = None,
        _return_as_generator: bool = False,
        **col_values: Any,
    ) -> Generator[TableSpecType, None, None] | list[TableSpecType]:
        if _return_as_generator:
            _queue: queue.SimpleQueue[TableSpecType | None] = queue.SimpleQueue()

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
                        _queue.put(entry)
                finally:
                    _queue.put(None)

            self._pool.submit(_inner)

            def _gen():
                while entry := _queue.get():
                    yield entry

            return _gen()

        else:
            return list(
                self._pool.submit(
                    super().orm_select_entries,
                    _distinct=_distinct,
                    _order_by=_order_by,
                    _limit=_limit,
                    **col_values,
                ).result()
            )

    @copy_callable_typehint(ORMBase.orm_insert_entries)
    def orm_insert_entries(self, *args, **kwargs) -> int:
        return self._pool.submit(super().orm_insert_entries, *args, **kwargs).result()

    @copy_callable_typehint(ORMBase.orm_insert_entry)
    def orm_insert_entry(self, *args, **kwargs) -> int:
        return self._pool.submit(super().orm_insert_entry, *args, **kwargs).result()

    def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]]] | None = None,
        _limit: int | None = None,
        _returning_cols: tuple[str, ...] | None | Literal["*"] = None,
        **cols_value: Any,
    ) -> int | list[TableSpecType]:
        # NOTE(20240708): currently we don't support generator for delete with RETURNING statement
        res = self._pool.submit(
            super().orm_delete_entries,
            _order_by=_order_by,
            _limit=_limit,
            _returning_cols=_returning_cols,
            **cols_value,
        ).result()

        if isinstance(res, Generator):
            return list(res)
        return res


class AsyncORMConnectionThreadPool(ORMConnectionThreadPool[TableSpecType]):

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
        self._run_coro_threadsafe = partial(
            asyncio.run_coroutine_threadsafe, loop=self._loop
        )
        """Run coroutine from thread and track result async."""

    async def _run_in_pool(
        self, func: Callable[P, RT], *args: P.args, **kwargs: P.kwargs
    ) -> RT:
        """Run normal function in threadpool and track the result async."""
        return await asyncio.wrap_future(
            self._pool.submit(func, *args, **kwargs),
            loop=self._loop,
        )

    def orm_select_entries_gen(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]], ...] | None = None,
        _limit: int | None = None,
        **col_values: Any,
    ) -> AsyncGenerator[TableSpecType, Any]:
        _async_queue: asyncio.Queue[TableSpecType | None] = asyncio.Queue()

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
                    self._run_coro_threadsafe(_async_queue.put(entry))
            finally:
                self._run_coro_threadsafe(_async_queue.put(None))

        self._pool.submit(_inner)

        async def _gen():
            while entry := await _async_queue.get():
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

    async def orm_create_table(
        self,
        *,
        allow_existed: bool = False,
        without_rowid: bool = False,
    ) -> None:
        return await self._run_in_pool(
            ORMBase.orm_create_table,
            self,
            allow_existed=allow_existed,
            without_rowid=without_rowid,
        )

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

    async def orm_insert_entries(self, _in: Iterable[TableSpecType]) -> int:
        return await self._run_in_pool(ORMBase.orm_insert_entries, self, _in)

    async def orm_insert_entry(self, _in: TableSpecType) -> int:
        return await self._run_in_pool(ORMBase.orm_insert_entry, self, _in)
