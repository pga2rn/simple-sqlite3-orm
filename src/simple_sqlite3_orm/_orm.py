from __future__ import annotations

import logging
import sqlite3
import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generator, Generic, Iterable, Literal, TypeVar
from weakref import WeakValueDictionary

from typing_extensions import ParamSpec

from simple_sqlite3_orm._sqlite_spec import ORDER_DIRECTION
from simple_sqlite3_orm._table_spec import TableSpec, TableSpecType

_parameterized_orm_cache: WeakValueDictionary[type[TableSpec], type["ORMBase[Any]"]] = (
    WeakValueDictionary()
)

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

        if _cached_type := _parameterized_orm_cache.get(params):
            return _std_GenericAlias(_cached_type, params)

        new_parameterized_ormbase: type[ORMBase] = type(
            f"{cls.__name__}[{params.__name__}]", (cls,), {}
        )
        new_parameterized_ormbase.orm_table_spec = params  # type: ignore
        _parameterized_orm_cache[params] = new_parameterized_ormbase
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
            _cur = con.execute(table_select_stmt, tuple(col_values.values()))
            _cur.row_factory = self.orm_table_spec.table_row_factory
            yield from _cur.fetchall()

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
                insert_stmt, tuple(_row.table_dump_astuple() for _row in _in)
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
            _cur = con.execute(insert_stmt, _in.table_dump_astuple())
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

            def _gen() -> Generator[TableSpecType, None, None]:
                with self._con as con:
                    _cur = con.execute(delete_stmt, tuple(cols_value.values()))
                    _cur.row_factory = self.orm_table_spec.table_row_factory
                    yield from _cur.fetchall()

            return _gen()
        else:
            with self._con as con:
                _cur = con.execute(delete_stmt, tuple(cols_value.values()))
                return _cur.rowcount

    def orm_cursor_wrapper(
        self, cursor: sqlite3.Cursor
    ) -> Generator[TableSpecType, None, None]:
        """A helper wrapper that setup row factory and yield from the <cursor>.

        This decorator is for advanced database operation which is expected to return a cursor that
            can be used to retrieve a list of entries from the table. User can use this wrapper to
            convert the cursor into a generator that yields the converted entries.

        Args:
            cursor (sqlite3.Cursor): A cursor which can be used to retrieve entries from.

        Returns:
            Generator[TableSpec, None, None]: A generator that can yield converted entries.
        """
        cursor.row_factory = self.orm_table_spec.table_row_factory
        yield from cursor.fetchall()


ORMBaseType = TypeVar("ORMBaseType", bound=ORMBase)
