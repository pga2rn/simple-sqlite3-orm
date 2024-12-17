from __future__ import annotations

import atexit
import queue
import threading
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, partial
from typing import Any, Generic, TypeVar
from weakref import WeakSet, WeakValueDictionary

from typing_extensions import Concatenate, ParamSpec

from simple_sqlite3_orm._orm._base import ORMBase, RowFactorySpecifier
from simple_sqlite3_orm._table_spec import TableSpec, TableSpecType
from simple_sqlite3_orm._types import ConnectionFactoryType
from simple_sqlite3_orm._utils import GenericAlias

_parameterized_orm_cache: WeakValueDictionary[
    tuple[type[ORMThreadPoolBase], type[TableSpec]], type[ORMThreadPoolBase[Any]]
] = WeakValueDictionary()

P = ParamSpec("P")
RT = TypeVar("RT")

_global_shutdown = False
_global_queue_weakset: WeakSet[queue.SimpleQueue] = WeakSet()


def _python_exit():
    global _global_shutdown
    _global_shutdown = True

    for _q in _global_queue_weakset:
        _q.put_nowait(_SENTINEL)


atexit.register(_python_exit)

_SENTINEL = object()


def _wrap_with_thread_ctx(func: Callable[Concatenate[ORMBase, P], RT]):
    def _wrapped(self: ORMThreadPoolBase, *args: P.args, **kwargs: P.kwargs) -> RT:
        def _in_thread() -> RT:
            _orm_base = self._thread_scope_orm
            return func(_orm_base, *args, **kwargs)

        return self._pool.submit(_in_thread).result()

    _wrapped.__doc__ = func.__doc__
    return _wrapped


def _wrap_generator_with_thread_ctx(
    func: Callable[Concatenate[ORMBase, P], Generator[TableSpecType]],
):
    def _wrapped(
        self: ORMThreadPoolBase, *args: P.args, **kwargs: P.kwargs
    ) -> Generator[TableSpecType]:
        _queue = queue.SimpleQueue()
        _global_queue_weakset.add(_queue)

        def _in_thread():
            _orm_base = self._thread_scope_orm
            global _global_shutdown
            try:
                for entry in func(_orm_base, *args, **kwargs):
                    if _global_shutdown:
                        return
                    _queue.put_nowait(entry)
            except Exception as e:
                _queue.put_nowait(e)
            finally:
                _queue.put_nowait(_SENTINEL)

        self._pool.submit(_in_thread)

        def _gen():
            while not _global_shutdown:
                entry = _queue.get()
                if entry is _SENTINEL:
                    return

                if isinstance(entry, Exception):
                    try:
                        raise entry
                    finally:
                        entry = None
                yield entry

        return _gen()

    _wrapped.__doc__ = func.__doc__
    return _wrapped


class ORMThreadPoolBase(Generic[TableSpecType]):
    """
    See https://www.sqlite.org/wal.html#concurrency for more details.

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

        # thread_scope ORMBase instances
        self._thread_id_orms: dict[int, ORMBase] = {}

        self._pool = ThreadPoolExecutor(
            max_workers=number_of_cons,
            initializer=partial(self._thread_initializer, con_factory, row_factory),
            thread_name_prefix=thread_name_prefix,
        )

    def __class_getitem__(cls, params: Any | type[Any] | type[TableSpecType]) -> Any:
        # just for convienience, passthrough anything that is not type[TableSpecType]
        #   to Generic's __class_getitem__ and return it.
        # Typically this is for subscript ORMBase with TypeVar or another Generic.
        if not (isinstance(params, type) and issubclass(params, TableSpec)):
            return super().__class_getitem__(params)  # type: ignore

        key = (cls, params)
        if _cached_type := _parameterized_orm_cache.get(key):
            return GenericAlias(_cached_type, params)

        new_parameterized_ormbase: type[ORMThreadPoolBase] = type(
            f"{cls.__name__}[{params.__name__}]", (cls,), {}
        )
        new_parameterized_ormbase.orm_table_spec = params  # type: ignore
        _parameterized_orm_cache[key] = new_parameterized_ormbase
        return GenericAlias(new_parameterized_ormbase, params)

    def _thread_initializer(self, con_factory, row_factory) -> None:
        """Prepare thread_scope ORMBase instance for this worker thread."""
        thread_id = threading.get_native_id()
        _orm = ORMBase[self.orm_table_spec](
            con_factory,
            self._orm_table_name,
            self._schema_name,
            row_factory=row_factory,
        )
        self._thread_id_orms[thread_id] = _orm

    @property
    def _thread_scope_orm(self) -> ORMBase[TableSpecType]:
        """Get thread scope ORMBase instance."""
        return self._thread_id_orms[threading.get_native_id()]

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
            for orm_base in self._thread_id_orms.values():
                orm_base._con.close()
        self._thread_id_orms = {}

    orm_execute = _wrap_with_thread_ctx(ORMBase.orm_execute)
    orm_executemany = _wrap_with_thread_ctx(ORMBase.orm_executemany)
    orm_executescript = _wrap_with_thread_ctx(ORMBase.orm_executescript)
    orm_create_table = _wrap_with_thread_ctx(ORMBase.orm_create_table)
    orm_create_index = _wrap_with_thread_ctx(ORMBase.orm_create_index)
    orm_select_entries = _wrap_generator_with_thread_ctx(ORMBase.orm_select_entries)
    orm_select_entry = _wrap_with_thread_ctx(ORMBase.orm_select_entry)
    orm_insert_entries = _wrap_with_thread_ctx(ORMBase.orm_insert_entries)
    orm_insert_entry = _wrap_with_thread_ctx(ORMBase.orm_insert_entry)
    orm_delete_entries = _wrap_with_thread_ctx(ORMBase.orm_delete_entries)
    orm_delete_entries_with_returning = _wrap_generator_with_thread_ctx(
        ORMBase.orm_delete_entries_with_returning
    )
    orm_select_all_with_pagination = _wrap_generator_with_thread_ctx(
        ORMBase.orm_select_all_with_pagination
    )
    orm_check_entry_exist = _wrap_with_thread_ctx(ORMBase.orm_check_entry_exist)


ORMThreadPoolBaseType = TypeVar("ORMThreadPoolBaseType", bound=ORMThreadPoolBase)
