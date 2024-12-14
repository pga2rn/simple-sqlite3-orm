from __future__ import annotations

import atexit
import logging
import sqlite3
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from queue import Queue, SimpleQueue
from typing import (
    Any,
    Callable,
    Generator,
    Iterable,
    Literal,
    TypeVar,
)

from typing_extensions import Concatenate, ParamSpec, deprecated

from simple_sqlite3_orm._orm._base import ORMBase
from simple_sqlite3_orm._sqlite_spec import INSERT_OR
from simple_sqlite3_orm._table_spec import TableSpecType
from simple_sqlite3_orm._types import RowFactoryType

logger = logging.getLogger(__name__)

P = ParamSpec("P")
RT = TypeVar("RT")

_global_shutdown = False


def _python_exit():
    global _global_shutdown
    _global_shutdown = True


atexit.register(_python_exit)


SENTINEL = object()


class Sqlite3ConnWithThread:
    """
    NOTE: this class is NOT thread safe and should only be used in the same thread.
    """

    def __init__(self) -> None:
        self._orm_pool: ORMThreadPoolBase = ""
        self._context_handler: _FuncCallInContextHandler | None = None

    def __getattribute__(self, name: str) -> Any:
        if name in [
            "_orm_pool",
            "_within_with",
            "__enter__",
            "__exit__",
        ]:
            return super().__getattribute__(name)

        _attr = getattr(sqlite3.Connection, name)
        if not callable(_attr):
            raise NotImplementedError(
                "non-function attr access is not supported for now"
            )

        ctx_handler = self._context_handler
        if not ctx_handler:
            return _wrapped_func_call(self, _attr)
        return partial(ctx_handler.call, _attr)

    def __enter__(self):
        if self._context_handler:
            raise ValueError(f"{self.__qualname__}: nested with context is not allowed")
        self._context_handler = _ctx_handler = _FuncCallInContextHandler(self)
        _ctx_handler.enter()
        return self

    def __exit__(self, *args, **kwargs):
        if not self._context_handler:
            raise ValueError(f"{self.__qualname__}: nested with context is not allowed")
        self._context_handler.exit()
        self._context_handler = None
        return False


def _wrapped_func_call(
    ctx: Sqlite3ConnWithThread,
    func: Callable[Concatenate[sqlite3.Connection, P], RT],
) -> Callable[P, RT]:
    def _outer(*args: P.args, **kwargs: P.kwargs) -> RT:
        _orm_pool = ctx._orm_pool
        if not _orm_pool:
            raise ValueError

        def _inner() -> RT:
            _conn = _orm_pool._con
            return func(_conn, *args, **kwargs)

        return _orm_pool._pool.submit(_inner).result()

    return _outer


class _FuncCallInContextHandler:
    def __init__(self, _ctx: Sqlite3ConnWithThread) -> None:
        self._ctx = _ctx
        self._req = Queue()
        self._resp = Queue()

    def enter(self):
        threading.Thread(target=self._context_worker, daemon=True).start()
        self.call(sqlite3.Connection.__enter__)

    def exit(self):
        self.call(sqlite3.Connection.__exit__, None, None, None)
        self._req.put_nowait(SENTINEL)

    def call(
        self,
        func: Callable[Concatenate[sqlite3.Connection, P], RT],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RT:
        self._req.put_nowait((func, args, kwargs))
        _res = self._resp.get()
        if _res is SENTINEL:
            raise RuntimeError

        if isinstance(_res, Exception):
            try:
                raise _res
            finally:
                _res = None
        return _res

    def _context_worker(self):
        # when we are at within context, execute all the following up func calls
        #   by this worker to ensure that all calls are done by the same thread.

        # get thread scope sqlite3 connection
        _conn = self._ctx._orm_pool._con

        while not _global_shutdown:
            _req = self._req.get()
            if _req is SENTINEL:
                return

            func, args, kwargs = _req
            try:
                self._resp.put_nowait(func(_conn, *args, **kwargs))
            except Exception as e:
                self._resp.put_nowait(e)


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
        _queue = SimpleQueue()

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

    def orm_select_entry(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]], ...] | None = None,
        _row_factory: RowFactoryType | None = None,
        **col_values: Any,
    ) -> Future[TableSpecType | Any | None]:
        return self._pool.submit(
            super().orm_select_entry,
            _distinct=_distinct,
            _order_by=_order_by,
            _row_factory=_row_factory,
            **col_values,
        )

    orm_select_entry.__doc__ = ORMBase.orm_select_entry.__doc__

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

    def orm_select_all_with_pagination(self, *, batch_size: int):
        raise NotImplementedError
