from __future__ import annotations

import asyncio
import atexit
import logging
from collections.abc import AsyncGenerator, Callable, Generator
from functools import cached_property
from typing import Any, Generic, TypeVar
from weakref import WeakValueDictionary

from typing_extensions import Concatenate, ParamSpec

from simple_sqlite3_orm._orm._base import RowFactorySpecifier
from simple_sqlite3_orm._orm._multi_thread import ORMBase, ORMThreadPoolBase
from simple_sqlite3_orm._table_spec import TableSpec, TableSpecType
from simple_sqlite3_orm._types import ConnectionFactoryType
from simple_sqlite3_orm._utils import GenericAlias

logger = logging.getLogger(__name__)

P = ParamSpec("P")
RT = TypeVar("RT")

_parameterized_orm_cache: WeakValueDictionary[
    tuple[type[AsyncORMBase], type[TableSpec]],
    type[AsyncORMBase[Any]],
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
    async def _wrapped(self: AsyncORMBase, *args: P.args, **kwargs: P.kwargs) -> RT:
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
    async def _wrapped(self: AsyncORMBase, *args: P.args, **kwargs: P.kwargs):
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


class AsyncORMBase(Generic[TableSpecType]):
    """
    NOTE: the supoprt for async ORM is experimental! The APIs might be changed a lot
        in the following releases.

    NOTE: AsyncORMBase is implemented with using ORMThreadPoolBase, but it is NOT a
        subclass of ORMThreadPoolBase!

    For the row_factory arg, please see ORMBase.__init__ for more details.
    """

    orm_table_spec: type[TableSpecType]
    _orm_table_name: str
    """table_name for the ORM. This can be used for pinning table_name when creating ORM object."""

    def __init__(
        self,
        table_name: str | None = None,
        schema_name: str | None = None,
        *,
        con_factory: ConnectionFactoryType,
        number_of_cons: int,
        thread_name_prefix: str = "",
        row_factory: RowFactorySpecifier = "table_spec",
    ) -> None:
        if table_name:
            self._orm_table_name = table_name
        if getattr(self, "_orm_table_name", None) is None:
            raise ValueError(
                "table_name must be either set by <table_name> init param, or by defining <_orm_table_name> attr."
            )
        self._schema_name = schema_name

        # setup the thread pool
        self._orm_threadpool = ORMThreadPoolBase[self.orm_table_spec](
            self._orm_table_name,
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

        new_parameterized_ormbase: type[AsyncORMBase] = type(
            f"{cls.__name__}[{params.__name__}]", (cls,), {}
        )
        new_parameterized_ormbase.orm_table_spec = params  # type: ignore
        _parameterized_orm_cache[key] = new_parameterized_ormbase
        return GenericAlias(new_parameterized_ormbase, params)

    @cached_property
    def orm_table_name(self) -> str:
        """The unique name of the table for use in sql statement.

        If multiple databases are attached to <con> and <schema_name> is availabe,
            return "<schema_name>.<table_name>", otherwise return <table_name>.
        """
        return (
            f"{self._schema_name}.{self._orm_table_name}"
            if self._schema_name
            else self._orm_table_name
        )

    def orm_pool_shutdown(self, *, wait=True, close_connections=True) -> None:
        """Shutdown the ORM connections thread pool used by this async ORM instance.

        It is safe to call this method multiple time.
        This method is NOT thread-safe, and should be called at the main thread,
            or the thread that creates this thread pool.

        Args:
            wait (bool, optional): Wait for threads join. Defaults to True.
            close_connections (bool, optional): Close all the connections. Defaults to True.
        """
        self._orm_threadpool.orm_pool_shutdown(
            wait=wait, close_connections=close_connections
        )

    orm_execute = _wrap_with_async_ctx(ORMBase.orm_execute)
    orm_executemany = _wrap_with_async_ctx(ORMBase.orm_executemany)
    orm_executescript = _wrap_with_async_ctx(ORMBase.orm_executescript)
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


AsyncORMBaseType = TypeVar("AsyncORMBaseType", bound=AsyncORMBase)
