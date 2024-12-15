from __future__ import annotations

import asyncio
import atexit
import logging
import sqlite3
from collections.abc import AsyncGenerator, Callable, Generator
from typing import Any, Generic, TypeVar
from weakref import WeakValueDictionary

from typing_extensions import Concatenate, ParamSpec

from simple_sqlite3_orm._orm._base import RowFactorySpecifier
from simple_sqlite3_orm._orm._multi_thread import ORMBase, ORMThreadPoolBase
from simple_sqlite3_orm._table_spec import TableSpec, TableSpecType
from simple_sqlite3_orm._utils import GenericAlias

logger = logging.getLogger(__name__)

P = ParamSpec("P")
RT = TypeVar("RT")

_parameterized_orm_cache: WeakValueDictionary[
    tuple[type[AsyncORMThreadPoolBase], type[TableSpec]],
    type[AsyncORMThreadPoolBase[Any]],
] = WeakValueDictionary()

_global_shutdown = False


def _python_exit():
    global _global_shutdown
    _global_shutdown = True


atexit.register(_python_exit)

_SENTINEL = object()


def _wrap_with_async_ctx(
    func: Callable[Concatenate[ORMBase, P], RT],
):
    async def _wrapped(
        self: AsyncORMThreadPoolBase, *args: P.args, **kwargs: P.kwargs
    ) -> RT:
        _orm_threadpool = self._orm_threadpool

        def _in_thread() -> RT:
            _orm_base = _orm_threadpool._thread_scope_orm
            return func(_orm_base, *args, **kwargs)

        return await asyncio.wrap_future(
            _orm_threadpool._pool.submit(_in_thread),
            loop=self._loop,
        )

    _wrapped.__doc__ = func.__doc__
    return _wrapped


def _wrap_generator_with_async_ctx(
    func: Callable[Concatenate[ORMBase, P], Generator[TableSpecType]],
):
    async def _wrapped(self: AsyncORMThreadPoolBase, *args: P.args, **kwargs: P.kwargs):
        _orm_threadpool = self._orm_threadpool
        _async_queue = asyncio.Queue()

        def _in_thread():
            global _global_shutdown
            _orm_base = _orm_threadpool._thread_scope_orm
            try:
                for entry in func(_orm_base, *args, **kwargs):
                    if _global_shutdown:
                        return
                    self._loop.call_soon_threadsafe(_async_queue.put_nowait, entry)
            except Exception as e:
                self._loop.call_soon_threadsafe(_async_queue.put_nowait, e)
            finally:
                self._loop.call_soon_threadsafe(_async_queue.put_nowait, _SENTINEL)

        self._orm_threadpool._pool.submit(_in_thread)

        async def _gen() -> AsyncGenerator[TableSpecType]:
            while not _global_shutdown:
                entry = await _async_queue.get()
                if entry is _SENTINEL:
                    return

                if isinstance(entry, Exception):
                    try:
                        raise entry
                    finally:
                        entry = None
                yield entry

        return _gen()

    return _wrapped


class AsyncORMThreadPoolBase(Generic[TableSpecType]):
    """
    NOTE: the supoprt for async ORM is experimental! The APIs might be changed a lot
        in the following releases.

    For the row_factory arg, please see ORMBase.__init__ for more details.
    """

    orm_table_spec: type[TableSpecType]

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
        # setup the thread pool
        self._orm_threadpool = ORMThreadPoolBase[self.orm_table_spec](
            table_name,
            schema_name,
            con_factory=con_factory,
            number_of_cons=number_of_cons,
            thread_name_prefix=thread_name_prefix,
            row_factory=row_factory,
        )

        self._loop = asyncio.get_running_loop()

    def __class_getitem__(cls, params: Any | type[Any] | type[TableSpecType]) -> Any:
        # just for convienience, passthrough anything that is not type[TableSpecType]
        #   to Generic's __class_getitem__ and return it.
        # Typically this is for subscript ORMBase with TypeVar or another Generic.
        if not (isinstance(params, type) and issubclass(params, TableSpec)):
            return super().__class_getitem__(params)  # type: ignore

        key = (cls, params)
        if _cached_type := _parameterized_orm_cache.get(key):
            return GenericAlias(_cached_type, params)

        new_parameterized_ormbase: type[AsyncORMThreadPoolBase] = type(
            f"{cls.__name__}[{params.__name__}]", (cls,), {}
        )
        new_parameterized_ormbase.orm_table_spec = params  # type: ignore
        _parameterized_orm_cache[key] = new_parameterized_ormbase
        return GenericAlias(new_parameterized_ormbase, params)

    orm_execute = _wrap_with_async_ctx(ORMBase.orm_execute)
    orm_create_table = _wrap_with_async_ctx(ORMBase.orm_create_table)
    orm_create_index = _wrap_with_async_ctx(ORMBase.orm_create_index)
    orm_select_entries = _wrap_generator_with_async_ctx(ORMBase.orm_select_entries)
    orm_select_entry = _wrap_with_async_ctx(ORMBase.orm_select_entry)
    orm_insert_entries = _wrap_with_async_ctx(ORMBase.orm_insert_entries)
    orm_insert_entry = _wrap_with_async_ctx(ORMBase.orm_insert_entry)
    orm_delete_entries = _wrap_with_async_ctx(ORMBase.orm_delete_entries)
    orm_delete_entries_with_returning = _wrap_generator_with_async_ctx(
        ORMBase.orm_delete_entries_with_returning
    )
    orm_select_all_with_pagination = _wrap_generator_with_async_ctx(
        ORMBase.orm_select_all_with_pagination
    )
    orm_check_entry_exist = _wrap_with_async_ctx(ORMBase.orm_check_entry_exist)
