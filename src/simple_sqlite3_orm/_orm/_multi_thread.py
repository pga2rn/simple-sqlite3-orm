from __future__ import annotations

import atexit
import logging
import queue
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import partial, wraps
from typing import (
    Any,
    Callable,
    Generator,
    Literal,
    TypeVar,
)

from typing_extensions import Concatenate, ParamSpec, deprecated

from simple_sqlite3_orm._orm._base import (
    ORMBase,
    RowFactorySpecifier,
    row_factory_setter,
)
from simple_sqlite3_orm._table_spec import TableSpecType

logger = logging.getLogger(__name__)

P = ParamSpec("P")
RT = TypeVar("RT")

_global_shutdown = False


def _python_exit():
    global _global_shutdown
    _global_shutdown = True


atexit.register(_python_exit)


def _wrap_with_thread_ctx(func: Callable[Concatenate[ORMThreadPoolBaseType, P], RT]):
    @wraps(func)
    def _wrapped(self: ORMThreadPoolBaseType, *args: P.args, **kwargs: P.kwargs) -> RT:
        return self._pool.submit(func, self, *args, **kwargs).result()

    return _wrapped


class ORMThreadPoolBase(ORMBase[TableSpecType]):
    """
    See https://www.sqlite.org/wal.html#concurrency for more details.

    For the row_factory arg, please see ORMBase.__init__ for more details.
    """

    def __init__(
        self,
        table_name: str,
        schema_name: str | None = None,
        *,
        con_factory: Callable[[], sqlite3.Connection],
        number_of_cons: int,
        thread_name_prefix: str = "",
        row_factory: RowFactorySpecifier = "table_spec",
    ) -> None:
        self._table_name = table_name
        self._schema_name = schema_name

        self._thread_id_cons: dict[int, sqlite3.Connection] = {}

        self._pool = ThreadPoolExecutor(
            max_workers=number_of_cons,
            initializer=partial(self._thread_initializer, con_factory, row_factory),
            thread_name_prefix=thread_name_prefix,
        )

    def _thread_initializer(self, con_factory, row_factory) -> None:
        thread_id = threading.get_native_id()
        self._thread_id_cons[thread_id] = con = con_factory()
        row_factory_setter(con, self.orm_table_spec, row_factory)

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

    orm_execute = _wrap_with_thread_ctx(ORMBase.orm_execute)
    orm_create_table = _wrap_with_thread_ctx(ORMBase.orm_create_table)
    orm_create_index = _wrap_with_thread_ctx(ORMBase.orm_create_index)

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

        return self._pool.submit(_inner).result()

    orm_select_entry = _wrap_with_thread_ctx(ORMBase.orm_select_entry)
    orm_insert_entries = _wrap_with_thread_ctx(ORMBase.orm_insert_entries)
    orm_insert_entry = _wrap_with_thread_ctx(ORMBase.orm_insert_entry)

    def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, Literal["ASC", "DESC"]]] | None = None,
        _limit: int | None = None,
        _returning_cols: tuple[str, ...] | None | Literal["*"] = None,
        **cols_value: Any,
    ) -> int | list[TableSpecType]:
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

        return self._pool.submit(_inner).result()

    orm_delete_entries.__doc__ = ORMBase.orm_delete_entries.__doc__

    def orm_select_all_with_pagination(self, *, batch_size: int):
        raise NotImplementedError

    orm_check_entry_exist = _wrap_with_thread_ctx(ORMBase.orm_check_entry_exist)


ORMThreadPoolBaseType = TypeVar("ORMThreadPoolBaseType", bound=ORMThreadPoolBase)
