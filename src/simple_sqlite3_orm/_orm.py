from __future__ import annotations

import logging
import sqlite3
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Generator,
    Iterable,
    Literal,
    TypeVar,
)
from weakref import WeakValueDictionary

from typing_extensions import ParamSpec, Self

from simple_sqlite3_orm._sqlite_spec import ORDER_DIRECTION
from simple_sqlite3_orm._table_spec import TableSpec, TableSpecType

_parameterized_orm_cache: WeakValueDictionary[
    tuple[type[ORMBase], type[TableSpec]], type[ORMBase[Any]]
] = WeakValueDictionary()

logger = logging.getLogger(__name__)

P = ParamSpec("P")

if sys.version_info >= (3, 9):
    from types import GenericAlias as _std_GenericAlias
else:
    from typing import List

    if not TYPE_CHECKING:
        _std_GenericAlias = type(List[int])
    else:

        class _std_GenericAlias(type(List)):
            def __new__(
                cls, _type: type[Any], _params: type[Any] | tuple[type[Any], ...]
            ):
                """For type check only, typing the _std_GenericAlias as GenericAlias."""


class ORMBase(Generic[TableSpecType]):
    """ORM for <TableSpecType>.

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

    def __class_getitem__(cls, params: Any | type[Any] | type[TableSpecType]) -> Any:
        # just for convienience, passthrough anything that is not type[TableSpecType]
        #   to Generic's __class_getitem__ and return it.
        # Typically this is for subscript ORMBase with TypeVar or another Generic.
        if not (isinstance(params, type) and issubclass(params, TableSpec)):
            return super().__class_getitem__(params)  # type: ignore

        key = (cls, params)
        if _cached_type := _parameterized_orm_cache.get(key):
            return _std_GenericAlias(_cached_type, params)

        new_parameterized_ormbase: type[ORMBase] = type(
            f"{cls.__name__}[{params.__name__}]", (cls,), {}
        )
        new_parameterized_ormbase.orm_table_spec = params  # type: ignore
        _parameterized_orm_cache[key] = new_parameterized_ormbase
        return _std_GenericAlias(new_parameterized_ormbase, params)

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
            _cur.row_factory = self.orm_table_spec.table_row_factory
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
                    _cur.row_factory = self.orm_table_spec.table_row_factory
                    yield from _cur

            return _gen()

        else:
            with self._con as con:
                _cur = con.execute(delete_stmt, tuple(cols_value.values()))
                return _cur.rowcount

    def orm_cursor_adaptor(self, cursor: sqlite3.Cursor) -> sqlite3.Cursor:
        """A helper wrapper that setup row_factory for the <cursor>.

        This decorator is for advanced database operation which is expected to return a cursor that
            can be used to retrieve a list of entries from the table.

        Args:
            cursor (sqlite3.Cursor): The cursor object.

        Returns:
            sqlite3.Cursor: The original cursor object, with row_factory set to <tablespec>.table_row_factory.
        """
        cursor.row_factory = self.orm_table_spec.table_row_factory
        return cursor


ORMBaseType = TypeVar("ORMBaseType", bound=ORMBase)


_WRAPPED_METHODS = set(
    [
        "orm_delete_entries",
        "orm_insert_entries",
        "orm_insert_entry",
        "orm_select_entries",
        "orm_create_index",
        "orm_create_table",
    ]
)


class ORMConnectionThreadPool(ORMBase[TableSpecType]):

    _pool: ThreadPoolExecutor

    @property
    def _con(self) -> sqlite3.Connection:
        """Get thread-specific sqlite3 connection."""
        return self._cons[self._thread_id_cons_id_map[threading.get_native_id()]]

    def _thread_initializer(self):
        thread_id = threading.get_native_id()
        self._thread_id_cons_id_map[thread_id] = len(self._thread_id_cons_id_map)

    def __new__(
        cls,
        table_name: str,
        schema_name: str | None = None,
        *,
        cons: list[sqlite3.Connection],
        thread_name_prefix: str = "",
    ) -> Self:
        new_obj = object.__new__(cls)

        worker_threads_num = len(cons)
        new_obj._pool = pool = ThreadPoolExecutor(
            max_workers=worker_threads_num,
            initializer=new_obj._thread_initializer,
            thread_name_prefix=thread_name_prefix,
        )

        for attr_name in dir(cls):
            if attr_name not in _WRAPPED_METHODS:
                continue

            attr = getattr(cls, attr_name)
            if callable(attr):

                def _wrapper(_new_object, _pool: ThreadPoolExecutor, _attr: Callable):
                    """Wrap the closure namespace."""
                    _bound_attr = _attr.__get__(_new_object)

                    @wraps(_attr)
                    def _inner(*args, **kwargs):
                        return _pool.submit(_bound_attr, *args, **kwargs).result()

                    return _inner

                setattr(new_obj, attr_name, _wrapper(new_obj, pool, attr))
        return new_obj

    def __init__(
        self,
        table_name: str,
        schema_name: str | None = None,
        *,
        cons: list[sqlite3.Connection],
        thread_name_prefix: str = "",
    ) -> None:
        self._table_name = table_name
        self._schema_name = schema_name
        self._cons = cons.copy()
        self._thread_id_cons_id_map: dict[int, int] = {}

    def orm_pool_shutdown(self, *, wait=True):
        self._pool.shutdown(wait=wait)
        for con in self._cons:
            con.close()
        self._cons = []
        self._thread_id_cons_id_map = {}

    if TYPE_CHECKING:

        def orm_delete_entries(
            self,
            *,
            _order_by: tuple[str | tuple[str, ORDER_DIRECTION]] | None = None,
            _limit: int | None = None,
            _returning_cols: tuple[str, ...] | Literal["*"] | None = None,
            **cols_value: Any,
        ) -> Future[int | Generator[TableSpecType, None, None]]: ...

        def orm_insert_entry(self, _in: TableSpecType) -> Future[int]: ...

        def orm_insert_entries(self, _in: Iterable[TableSpecType]) -> Future[int]: ...

        def orm_select_entries(
            self,
            *,
            _distinct: bool = False,
            _order_by: tuple[str | tuple[str, ORDER_DIRECTION], ...] | None = None,
            _limit: int | None = None,
            **col_values: Any,
        ) -> Future[Generator[TableSpecType, None, None]]: ...

        def orm_create_index(
            self,
            *,
            index_name: str,
            index_keys: tuple[str, ...],
            allow_existed: bool = False,
            unique: bool = False,
        ) -> Future[None]: ...

        def orm_create_table(
            self,
            *,
            allow_existed: bool = False,
            without_rowid: bool = False,
        ) -> Future[None]: ...
