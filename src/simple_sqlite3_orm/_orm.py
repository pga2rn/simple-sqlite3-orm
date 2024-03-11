from __future__ import annotations

import sqlite3
import sys
from typing import Any, Generator, Generic, Iterable, Optional, TypeVar
from weakref import WeakValueDictionary

from typing_extensions import Self

from simple_sqlite3_orm._table_spec import TableSpecType, TableSpec
from simple_sqlite3_orm._sqlite_spec import ORDER_DIRECTION

ORMType = TypeVar("ORMType", bound="ORMBase")

if sys.version_info >= (3, 9):  # Typing for weak dictionaries is available since 3.9
    GenericTypesCache = WeakValueDictionary[tuple[type[ORMType]], type[TableSpecType]]
else:
    GenericTypesCache = WeakValueDictionary

_parameterized_orm_cache = GenericTypesCache()


class ORMBase(Generic[TableSpecType]):

    table_spec: type[TableSpecType]

    def __init__(
        self,
        con: sqlite3.Connection,
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> None:
        self.table_name = table_name
        self.schema_name = schema_name
        self._con = con

    def __class_getitem__(
        cls: type[Self], params: Any | type[TableSpecType]
    ) -> type[Self]:
        if not (isinstance(params, type) and issubclass(params, TableSpec)):
            raise TypeError

        if _cached_type := _parameterized_orm_cache.get(params):
            return _cached_type

        _new_parameterized_container: Any = type(
            f"{cls.__name__}[{params.__name__}]",
            (cls,),
            {"table_spec": params},
        )
        _parameterized_orm_cache[params] = _new_parameterized_container
        return _new_parameterized_container

    def _get_table_name(self) -> str:
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
                    self._get_table_name(),
                    if_not_exists=allow_existed,
                    without_rowid=without_rowid,
                )
            )

    def create_index(
        self,
        index_name: str,
        allow_existed: bool = False,
        unique: bool = False,
        *cols: str,
    ) -> None:
        _index_create_stmt = self.table_spec.table_create_index_stmt(
            self._get_table_name(),
            index_name,
            unique=unique,
            if_not_exists=allow_existed,
            *cols,
        )
        with self._con as con:
            con.execute(_index_create_stmt)

    def select_entries(
        self,
        distinct: bool = False,
        order_by: Optional[Iterable[str]] = None,
        limit: Optional[int] = None,
        **col_values: Any,
    ) -> Generator[TableSpecType, None, None]:
        with self._con as con:
            _cur = con.execute(
                self.table_spec.table_select_stmt(
                    self._get_table_name(),
                    distinct=distinct,
                    order_by=order_by,
                    limit=limit,
                    **col_values,
                )
            )
            _cur.row_factory = self.table_spec.table_row_factory
            yield from _cur.fetchall()

    def insert_entries(self, _in: TableSpecType | Iterable[TableSpecType]) -> int:
        _insert_stmt = self.table_spec.table_insert_stmt(self._get_table_name())
        with self._con as con:
            if isinstance(_in, self.table_spec):
                _cur = con.execute(_insert_stmt, _in.table_row_astuple())
                return _cur.rowcount

            _cur = con.executemany(
                _insert_stmt, tuple(_row.table_row_astuple() for _row in _in)
            )
            return _cur.rowcount

    def delete_entries(
        self,
        limit: Optional[int] = None,
        order_by: Optional[Iterable[str | tuple[str, ORDER_DIRECTION]]] = None,
        returning: Optional[bool] = None,
        **cols_value: Any,
    ) -> int | Generator[TableSpecType, None, None]:
        _delete_stmt = self.table_spec.table_delete_stmt(
            self._get_table_name(),
            limit=limit,
            order_by=order_by,
            returning=returning,
            **cols_value,
        )
        with self._con as con:
            _cur = con.execute(_delete_stmt, tuple(cols_value.values()))
            if returning:
                _cur.row_factory = self.table_spec.table_row_factory
                yield from _cur.fetchall()
            return _cur.rowcount
