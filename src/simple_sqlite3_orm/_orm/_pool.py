from __future__ import annotations

import asyncio
import atexit
import contextlib
import queue
import threading
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, partial
from typing import TYPE_CHECKING, AsyncGenerator, TypeVar
from weakref import WeakSet

from typing_extensions import Concatenate, ParamSpec, Self

from simple_sqlite3_orm._orm._base import ORMBase, ORMCommonBase, RowFactorySpecifier
from simple_sqlite3_orm._orm._utils import parameterized_class_getitem
from simple_sqlite3_orm._table_spec import TableSpecType
from simple_sqlite3_orm._typing import ConnectionFactoryType

P = ParamSpec("P")
RT = TypeVar("RT")

_global_shutdown = False
_global_queue_weakset: WeakSet[queue.Queue] = WeakSet()
MAX_QUEUE_SIZE = 128

_ERR_MSG_SUBMIT_ON_SHUTDOWN = "cannot schedule new task on pool shutdown"


def _python_exit():  # pragma: no cover
    global _global_shutdown
    _global_shutdown = True

    for _q in _global_queue_weakset:
        # drain the queue to unblock the producer.
        # Once the producer is unblocked, as the global_shutdown is set to True,
        #  it will directly return.
        with contextlib.suppress(queue.Empty):
            while not _q.empty():
                _q.get_nowait()

        # then wake up the consumer
        with contextlib.suppress(queue.Full):
            _q.put(_SENTINEL, block=True, timeout=0.1)


atexit.register(_python_exit)

_SENTINEL = object()


def _wrap_with_thread_ctx(func: Callable[Concatenate[ORMBase, P], RT]):
    def _wrapped(self: ORMThreadPoolBase, *args: P.args, **kwargs: P.kwargs) -> RT:
        if self._closed:  # pragma: no cover
            raise RuntimeError(_ERR_MSG_SUBMIT_ON_SHUTDOWN)

        def _in_thread() -> RT:
            _orm_base = self._thread_scope_orm
            return func(_orm_base, *args, **kwargs)

        return self._pool.submit(_in_thread).result()

    _wrapped.__doc__ = func.__doc__
    return _wrapped


def _wrap_with_async_ctx(
    func: Callable[Concatenate[ORMBase, P], RT],
):
    async def _wrapped(self: AsyncORMBase, *args: P.args, **kwargs: P.kwargs) -> RT:
        if self._closed:  # pragma: no cover
            raise RuntimeError(_ERR_MSG_SUBMIT_ON_SHUTDOWN)

        def _in_thread() -> RT:
            _orm_base = self._thread_scope_orm
            return func(_orm_base, *args, **kwargs)

        return await asyncio.wrap_future(self._pool.submit(_in_thread), loop=self._loop)

    _wrapped.__doc__ = func.__doc__
    return _wrapped


def _wrap_generator_with_thread_ctx(
    func: Callable[Concatenate[ORMBase, P], Generator[RT]],
):
    def _wrapped(
        self: ORMThreadPoolBase, *args: P.args, **kwargs: P.kwargs
    ) -> Generator[RT]:
        if self._closed:  # pragma: no cover
            raise RuntimeError(_ERR_MSG_SUBMIT_ON_SHUTDOWN)

        _queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        _caller_exit = threading.Event()
        _global_queue_weakset.add(_queue)

        def _in_thread():
            _orm_base = self._thread_scope_orm
            try:
                for entry in func(_orm_base, *args, **kwargs):
                    if _global_shutdown or self._closed or _caller_exit.is_set():
                        return
                    _queue.put(entry)
            except BaseException as e:
                _queue.put(e)
            finally:
                _queue.put(_SENTINEL)

        self._pool.submit(_in_thread)
        return self._caller_gen(_queue, _caller_exit)

    _wrapped.__doc__ = func.__doc__
    return _wrapped


def _wrap_generator_with_async_ctx(
    func: Callable[Concatenate[ORMBase, P], Generator[RT]],
):
    async def _wrapped(self: AsyncORMBase, *args: P.args, **kwargs: P.kwargs):
        if self._closed:  # pragma: no cover
            raise RuntimeError(_ERR_MSG_SUBMIT_ON_SHUTDOWN)

        _queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        _caller_exit = threading.Event()
        _global_queue_weakset.add(_queue)
        _se = asyncio.Semaphore()

        def _in_thread():
            _orm_base = self._thread_scope_orm
            _bound_cb = self._loop.call_soon_threadsafe
            try:
                for entry in func(_orm_base, *args, **kwargs):
                    if _global_shutdown or self._closed or _caller_exit.is_set():
                        return
                    _queue.put(entry)
                    _bound_cb(_se.release)
            except BaseException as e:
                _queue.put(e)
                _bound_cb(_se.release)
            finally:
                _queue.put(_SENTINEL)
                _bound_cb(_se.release)

        self._pool.submit(_in_thread)
        return self._async_caller_gen(_queue, _se, _caller_exit)

    _wrapped.__doc__ = func.__doc__
    return _wrapped


class ORMThreadPoolBase(ORMCommonBase[TableSpecType]):
    """
    See https://www.sqlite.org/wal.html#concurrency for more details.

    For the row_factory arg, please see ORMBase.__init__ for more details.
    """

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

        self._num_of_cons = number_of_cons
        self._pool = ThreadPoolExecutor(
            max_workers=number_of_cons,
            initializer=partial(self._thread_initializer, con_factory, row_factory),
            thread_name_prefix=thread_name_prefix,
        )

        self._closed = False

    __class_getitem__ = classmethod(parameterized_class_getitem)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exec_type, exc_val, exc_tb):
        self.orm_pool_shutdown(wait=True, close_connections=True)
        return False

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

    def _caller_gen(
        self, _queue: queue.Queue[RT], _caller_exit: threading.Event
    ) -> Generator[RT]:
        try:
            while not _global_shutdown and not self._closed:
                entry = _queue.get()
                if entry is _SENTINEL:
                    return

                if isinstance(entry, Exception):
                    try:
                        raise entry
                    finally:
                        entry = None
                yield entry
        finally:
            # on shutdown, drain the _queue to unblock in_thread
            with contextlib.suppress(queue.Empty):
                while True:
                    _queue.get_nowait()
            _caller_exit.set()

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

    def _worker_shutdown(
        self, shutdown_barrier: threading.Barrier, shutdown_lock: threading.Lock
    ):
        shutdown_barrier.wait()  # wait for all work threads get the worker_shutdown
        with shutdown_lock:
            if _thread_scope_orm := self._thread_id_orms.pop(
                threading.get_native_id(), None
            ):
                _thread_scope_orm._con.close()

    def orm_pool_shutdown(self, *, wait=True, close_connections=True) -> None:
        """Shutdown the ORM connections thread pool.

        It is safe to call this method multiple time.
        This method is NOT thread-safe, and should be called at the main thread,
            or the thread that creates this thread pool.

        Args:
            wait (bool, optional): Wait for threads join. Defaults to True.
            close_connections (bool, optional): Close all the connections. Defaults to True.
        """
        self._closed = True
        if self._pool._shutdown:  # pragma: no cover
            # NOTE: ThreadPoolExecutor's shutdown method itself can be call multiple times
            return self._pool.shutdown(wait=wait)

        if close_connections:
            _barrier = threading.Barrier(self._num_of_cons + 1)
            _lock = threading.Lock()
            for _ in range(self._num_of_cons):
                self._pool.submit(self._worker_shutdown, _barrier, _lock)
            _barrier.wait()
        self._pool.shutdown(wait=wait)

    orm_bootstrap_db = _wrap_with_thread_ctx(ORMBase.orm_bootstrap_db)
    orm_execute = _wrap_with_thread_ctx(ORMBase.orm_execute)
    orm_execute_gen = _wrap_generator_with_thread_ctx(ORMBase.orm_execute_gen)
    orm_executemany = _wrap_with_thread_ctx(ORMBase.orm_executemany)
    orm_executescript = _wrap_with_thread_ctx(ORMBase.orm_executescript)
    orm_create_table = _wrap_with_thread_ctx(ORMBase.orm_create_table)
    orm_create_index = _wrap_with_thread_ctx(ORMBase.orm_create_index)
    orm_select_entries = _wrap_generator_with_thread_ctx(ORMBase.orm_select_entries)
    orm_select_entry = _wrap_with_thread_ctx(ORMBase.orm_select_entry)
    orm_insert_entries = _wrap_with_thread_ctx(ORMBase.orm_insert_entries)
    orm_insert_mappings = _wrap_with_thread_ctx(ORMBase.orm_insert_mappings)
    orm_insert_mapping = _wrap_with_thread_ctx(ORMBase.orm_insert_mapping)
    orm_insert_entry = _wrap_with_thread_ctx(ORMBase.orm_insert_entry)
    orm_update_entries = _wrap_with_thread_ctx(ORMBase.orm_update_entries)
    orm_update_entries_many = _wrap_with_thread_ctx(ORMBase.orm_update_entries_many)
    orm_delete_entries = _wrap_with_thread_ctx(ORMBase.orm_delete_entries)
    orm_delete_entries_with_returning = _wrap_generator_with_thread_ctx(
        ORMBase.orm_delete_entries_with_returning
    )
    orm_select_all_with_pagination = _wrap_generator_with_thread_ctx(
        ORMBase.orm_select_all_with_pagination
    )
    orm_check_entry_exist = _wrap_with_thread_ctx(ORMBase.orm_check_entry_exist)


ORMThreadPoolBaseType = TypeVar("ORMThreadPoolBaseType", bound=ORMThreadPoolBase)


class AsyncORMBase(ORMThreadPoolBase[TableSpecType]):
    """
    NOTE: the supoprt for async ORM is experimental! The APIs might be changed a lot
        in the following releases.

    For the row_factory arg, please see ORMBase.__init__ for more details.
    """

    _loop: asyncio.AbstractEventLoop
    """Bound event loop when instanitiate this pool."""

    if not TYPE_CHECKING:

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self._loop = asyncio.get_running_loop()

    async def _async_caller_gen(
        self,
        _queue: queue.Queue[RT],
        _se: asyncio.Semaphore,
        _caller_exit: threading.Event,
    ) -> AsyncGenerator[RT]:
        try:
            while not _global_shutdown and not self._closed:
                await _se.acquire()
                entry = _queue.get()

                if entry is _SENTINEL:
                    return

                if isinstance(entry, Exception):
                    try:
                        raise entry
                    finally:
                        entry = None
                yield entry
        finally:
            # on shutdown, drain the _queue to unblock in_thread
            with contextlib.suppress(queue.Empty):
                while True:
                    _queue.get_nowait()
            _caller_exit.set()

    orm_bootstrap_db = _wrap_with_async_ctx(ORMBase.orm_bootstrap_db)
    orm_execute = _wrap_with_async_ctx(ORMBase.orm_execute)
    orm_execute_gen = _wrap_generator_with_async_ctx(ORMBase.orm_execute_gen)
    orm_executemany = _wrap_with_async_ctx(ORMBase.orm_executemany)
    orm_executescript = _wrap_with_async_ctx(ORMBase.orm_executescript)
    orm_create_table = _wrap_with_async_ctx(ORMBase.orm_create_table)
    orm_create_index = _wrap_with_async_ctx(ORMBase.orm_create_index)
    orm_select_entries = _wrap_generator_with_async_ctx(ORMBase.orm_select_entries)
    orm_select_entry = _wrap_with_async_ctx(ORMBase.orm_select_entry)
    orm_insert_entries = _wrap_with_async_ctx(ORMBase.orm_insert_entries)
    orm_insert_mappings = _wrap_with_async_ctx(ORMBase.orm_insert_mappings)
    orm_insert_mapping = _wrap_with_async_ctx(ORMBase.orm_insert_mapping)
    orm_insert_entry = _wrap_with_async_ctx(ORMBase.orm_insert_entry)
    orm_update_entries = _wrap_with_async_ctx(ORMBase.orm_update_entries)
    orm_update_entries_many = _wrap_with_async_ctx(ORMBase.orm_update_entries_many)
    orm_delete_entries = _wrap_with_async_ctx(ORMBase.orm_delete_entries)
    orm_delete_entries_with_returning = _wrap_generator_with_async_ctx(
        ORMBase.orm_delete_entries_with_returning
    )
    orm_select_all_with_pagination = _wrap_generator_with_async_ctx(
        ORMBase.orm_select_all_with_pagination
    )
    orm_check_entry_exist = _wrap_with_async_ctx(ORMBase.orm_check_entry_exist)


AsyncORMBaseType = TypeVar("AsyncORMBaseType", bound=AsyncORMBase)
