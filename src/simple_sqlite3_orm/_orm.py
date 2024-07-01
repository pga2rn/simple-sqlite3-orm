from __future__ import annotations

import logging
import sqlite3
import sys
from typing import Any, TYPE_CHECKING, Generator, Generic, Iterable
from weakref import WeakValueDictionary

from typing_extensions import Self

from simple_sqlite3_orm._sqlite_spec import ORDER_DIRECTION
from simple_sqlite3_orm._table_spec import TableSpec, TableSpecType

_parameterized_orm_cache: WeakValueDictionary[type[TableSpec], type["ORMBase[Any]"]] = (
    WeakValueDictionary()
)

logger = logging.getLogger(__name__)

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

    table_spec: type[TableSpecType]

    def __init__(
        self,
        con: sqlite3.Connection,
        table_name: str,
        schema_name: str | None = None,
    ) -> None:
        self.table_name = table_name
        self.schema_name = schema_name
        self._con = con

    def __class_getitem__(
        cls: type[Self], params: Any | type[Any] | type[TableSpecType]
    ) -> Any:
        # just for convienience, passthrough anything that is not type[TableSpecType]
        #   to Generic's __class_getitem__ and return it.
        # Typically this is for subscript ORMBase with TypeVar or another Generic.
        if not (isinstance(params, type) and issubclass(params, TableSpec)):
            return super().__class_getitem__(params)  # type: ignore

        if _cached_type := _parameterized_orm_cache.get(params):
            return _std_GenericAlias(_cached_type, params)

        _new_parameterized_container: Any = type(
            f"{cls.__name__}[{params.__name__}]",
            (cls,),
            {"table_spec": params},
        )
        _parameterized_orm_cache[params] = _new_parameterized_container
        return _std_GenericAlias(_new_parameterized_container, params)

    @property
    def con(self) -> sqlite3.Connection:
        """A reference to the underlying sqlite3.Connection.

        This is for advanced database execution.
        """
        return self._con

    def get_table_name(self) -> str:
        """Get the unique name for the table from <con>.

        If multiple databases are attached to <con> and <schema_name> is availabe,
            return "<schema_name>.<table_name>", otherwise return <table_name>.
        """
        return (
            f"{self.schema_name}.{self.table_name}"
            if self.schema_name
            else self.table_name
        )

    def create_table(
        self,
        allow_existed: bool = False,
        without_rowid: bool = False,
    ) -> None:
        with self._con as con:
            con.execute(
                self.table_spec.table_create_stmt(
                    self.get_table_name(),
                    if_not_exists=allow_existed,
                    without_rowid=without_rowid,
                )
            )

    def create_index(
        self,
        index_name: str,
        *cols: str,
        allow_existed: bool = False,
        unique: bool = False,
    ) -> None:
        index_create_stmt = self.table_spec.table_create_index_stmt(
            self.get_table_name(),
            index_name,
            unique=unique,
            if_not_exists=allow_existed,
            index_cols=list(cols),
        )
        logger.debug(f"{index_create_stmt=}")
        with self._con as con:
            con.execute(index_create_stmt)

    def select_entries(
        self,
        distinct: bool = False,
        order_by: Iterable[str] | None = None,
        limit: int | None = None,
        **col_values: Any,
    ) -> Generator[TableSpecType, None, None]:
        table_select_stmt = self.table_spec.table_select_stmt(
            self.get_table_name(),
            distinct=distinct,
            order_by=order_by,
            limit=limit,
            where_cols=list(col_values),
        )
        logger.debug(f"{table_select_stmt=}")

        with self._con as con:
            _cur = con.execute(table_select_stmt, tuple(col_values.values()))
            _cur.row_factory = self.table_spec.table_row_factory
            yield from _cur.fetchall()

    def insert_entries(self, _in: TableSpecType | Iterable[TableSpecType]) -> int:
        """Insert entry/entries into this table.

        Args:
            _in (TableSpecType | Iterable[TableSpecType]): The instance of entry(or a list of entries)
                to be inserted into the table.

        Raises:
            ValueError: On invalid types of _in.

        Returns:
            int: Number of inserted entries.
        """
        insert_stmt = self.table_spec.table_insert_stmt(self.get_table_name())
        logger.debug(f"{insert_stmt=}")

        with self._con as con:
            if isinstance(_in, tuple):
                _cur = con.executemany(
                    insert_stmt, tuple(_row.table_row_astuple() for _row in _in)
                )
                return _cur.rowcount
            elif isinstance(_in, self.table_spec):
                _cur = con.execute(insert_stmt, _in.table_row_astuple())
                return _cur.rowcount
            else:
                raise ValueError(
                    "invalid input type, expects tuple or instance of TableSpecType"
                )

    def delete_entries(
        self,
        limit: int | None = None,
        order_by: Iterable[str | tuple[str, ORDER_DIRECTION]] | None = None,
        returning: bool | None = None,
        **cols_value: Any,
    ) -> int | Generator[TableSpecType, None, None]:
        delete_stmt = self.table_spec.table_delete_stmt(
            self.get_table_name(),
            limit=limit,
            order_by=order_by,
            returning=returning,
            where_cols=list(cols_value),
        )
        logger.debug(f"{delete_stmt=}")

        if returning:

            def _gen() -> Generator[TableSpecType, None, None]:
                with self._con as con:
                    _cur = con.execute(delete_stmt, tuple(cols_value.values()))
                    _cur.row_factory = self.table_spec.table_row_factory
                    yield from _cur.fetchall()

            return _gen()
        else:
            with self._con as con:
                _cur = con.execute(delete_stmt, tuple(cols_value.values()))
                return _cur.rowcount
