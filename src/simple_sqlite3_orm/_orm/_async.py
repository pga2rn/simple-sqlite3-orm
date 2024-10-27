from __future__ import annotations

import asyncio
import logging
import sqlite3
import sys
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Iterable,
    Literal,
    TypeVar,
)

from typing_extensions import ParamSpec, deprecated

from simple_sqlite3_orm._orm import _multi_thread as _orm_multi_thread
from simple_sqlite3_orm._orm._multi_thread import ORMBase, ORMThreadPoolBase
from simple_sqlite3_orm._sqlite_spec import INSERT_OR
from simple_sqlite3_orm._table_spec import TableSpecType

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
                    if _orm_multi_thread._global_shutdown:
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
