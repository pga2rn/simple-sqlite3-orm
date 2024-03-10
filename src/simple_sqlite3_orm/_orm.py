from __future__ import annotations

import sqlite3
from io import StringIO
from typing import Any, Iterable, Optional

from pydantic import BaseModel
from typing_extensions import Self

from simple_sqlite3_orm._db import (
    INSERT_OR,
    ORDER_DIRECTION,
    SQLiteBuiltInFuncs,
    SQLiteStorageClass,
)
from simple_sqlite3_orm._utils import (
    ConstrainRepr,
    TypeAffinityRepr,
    check_cols,
    filter_with_order,
)


class ORMBase(BaseModel):
    orm_check_cols = classmethod(check_cols)
    _filter_with_order = classmethod(filter_with_order)

    @classmethod
    def orm_dump_column(cls, column_name: str) -> str:
        """Dump the column statement for table creation."""
        _datatype_name, _constrain = "", ""
        for _metadata in cls.model_fields[column_name].metadata:
            if isinstance(_metadata, TypeAffinityRepr):
                _datatype_name = _metadata
            elif isinstance(_metadata, ConstrainRepr):
                _constrain = _metadata
        assert _datatype_name, "data affinity must be set"
        return f"{column_name} {_datatype_name} {_constrain}".strip()

    @classmethod
    def simple_create_table_stmt(
        cls,
        table_name: str,
        *,
        if_not_exists: bool = False,
        strict: bool = False,
        temporary: bool = False,
        without_rowid: bool = False,
    ) -> str:
        """Get create table statement for this table spec class.

        Check https://www.sqlite.org/lang_createtable.html for more details.
        """

        _cols_spec = ",".join(
            cls.orm_dump_column(col_name) for col_name in cls.model_fields
        )
        cols_stmt = f"({_cols_spec}) "
        return (
            f"CREATE {'TEMPORARY ' if temporary else ''}"
            f"{'IF NOT EXISTS ' if if_not_exists else ''}"
            f"TABLE {table_name} ({cols_stmt}) "
            f"{'WITHOUT ROWID ' if without_rowid else ''}"
            f"{'STRICT ' if strict else ''};"
        )

    @classmethod
    def simple_create_index_stmt(
        cls,
        table_name: str,
        index_name: str,
        *cols: str | tuple[str, ORDER_DIRECTION],
        if_not_exists: bool = False,
        unique: bool = False,
    ) -> str:
        """Get index create statement for this table spec class.

        Check https://www.sqlite.org/lang_createindex.html for more details.
        """
        assert cols, "at least one col should be specified for an index"

        _indexed_cols: list[str] = []
        for _input in cols:
            if isinstance(_input, tuple):
                _col, _order = _input
                cls.orm_check_cols(_col)
                _indexed_cols.append(f"{_col} {_order}")
            else:
                _col = _input
                cls.orm_check_cols(_col)
                _indexed_cols.append(_col)
        indexed_columns_stmt = f"({','.join(_indexed_cols)}) "

        return (
            f"CREATE {'UNIQUE' if unique else ''} INDEX "
            f"{'IF NOT EXISTS' if if_not_exists else ''} {index_name} "
            f"ON {table_name} {indexed_columns_stmt};"
        )

    @classmethod
    def row_factory(
        cls, _cursor: sqlite3.Cursor, _row: tuple[Any, ...] | sqlite3.Row
    ) -> Self:
        """row_factory implement for used in sqlite3 connection."""
        _fields = [col[0] for col in _cursor.description]
        return cls.model_construct(**dict(zip(_fields, _row)))

    def orm_as_tuple(self, *cols: str) -> tuple[SQLiteStorageClass, ...]:
        """Dump self to a tuple of col values.

        Args:
            *cols: which cols to export, if not specified, export all cols.

        Returns:
            A tuple of col values from this row.
        """
        if not cols:
            return tuple(getattr(self, _col) for _col in self.model_fields)
        return tuple(getattr(self, _col) for _col in self._filter_with_order(*cols))

    @classmethod
    def simple_insert_entry_stmt(
        cls,
        insert_into: str,
        *cols: str,
        insert_default: bool = False,
        insert_select: Optional[str] = None,
        or_option: Optional[INSERT_OR] = None,
        returning: bool | str = False,
    ) -> str:
        """Get sql for inserting row(s) into <table_name>.

        Check https://www.sqlite.org/lang_insert.html for more details.
        """
        _or_option_stmt = ""
        if or_option:
            _or_option_stmt = f"OR {or_option.upper()}"
        insert_stmt = f"INSERT {_or_option_stmt} INTO {insert_into} "

        if cols:
            cols_specify_stmt = f"({','.join(cls._filter_with_order(*cols))}) "
            values_specify_stmt = f"VALUES ({','.join(['?'] * len(cols))}) "
        else:
            cols_specify_stmt = ""
            values_specify_stmt = f"VALUES ({','.join(['?'] * len(cls.model_fields))}) "

        returning_stmt = (
            f"RETURNING {'*' if returning is True else returning} " if returning else ""
        )

        with StringIO() as buffer:
            buffer.write(insert_stmt)
            buffer.write(cols_specify_stmt)

            if insert_default:
                buffer.write("DEFAULT VALUES ")
            if insert_select:
                buffer.write(f"{insert_select} ")
            else:
                buffer.write(values_specify_stmt)

            buffer.write(returning_stmt)
            buffer.write(";")
            return buffer.getvalue()

    @classmethod
    def simple_select_entry_stmt(
        cls,
        select_from: str,
        /,
        *select_cols: str,
        distinct: bool = False,
        function: Optional[SQLiteBuiltInFuncs] = None,
        group_by: Optional[Iterable[str]] = None,
        order_by: Optional[Iterable[str | tuple[str, ORDER_DIRECTION]]] = None,
        limit: Optional[int | str] = None,
        **col_values: Any,
    ) -> str:
        """Get sql for getting row(s) from <table_name>, optionally with
            where condition specified by <col_values>.

        Check https://www.sqlite.org/lang_select.html for more details.
        """
        _select_target = "*"
        if select_cols:
            _select_target = cls._filter_with_order(*select_cols)
        if function:
            _select_target = f"{function}({_select_target})"
        select_stmt = f"SELECT {'DISTINCT ' if distinct else ''}{_select_target} "
        from_stmt = f"FROM {select_from} "

        where_stmt = ""
        if col_values:
            _conditions: list[str] = []
            for _col, _value in col_values.items():
                if _col in cls.model_fields:
                    _conditions.append(f"{_col}={_value}")
            where_stmt = f"WHERE {' AND '.join(_conditions)} "

        group_by_stmt = f"GROUP BY {','.join(group_by)} " if group_by else ""

        order_by_stmt = ""
        if order_by:
            _order_by_stmts: list[str] = []
            for _item in order_by:
                if isinstance(_item, tuple):
                    _col, _direction = _item
                    cls.orm_check_cols(_col)
                    _order_by_stmts.append(f"{_col} {_direction}")
                else:
                    _order_by_stmts.append(_item)
            order_by_stmt = f"{','}.join(_order_by_stmts)"

        limit_stmt = f"LIMIT {limit} " if limit is not None else ""

        with StringIO() as buffer:
            buffer.write(select_stmt)
            buffer.write(from_stmt)
            buffer.write(where_stmt)
            buffer.write(group_by_stmt)
            buffer.write(order_by_stmt)
            buffer.write(limit_stmt)
            buffer.write(";")
            return buffer.getvalue()

    @classmethod
    def simple_delete_entry_stmt(
        cls,
        delete_from: str,
        limit: Optional[int | str] = None,
        order_by: Optional[Iterable[str | tuple[str, ORDER_DIRECTION]]] = None,
        returning: Optional[bool | str] = None,
        **col_values: Any,
    ) -> str:
        """Get sql for deleting row(s) from <table_name> with specifying col value(s).

        Check https://www.sqlite.org/lang_delete.html for more details.
        """
        delete_from_stmt = f"DELETE {delete_from} "
        where_stmt = ""
        if col_values:
            cls.orm_check_cols(*col_values)
            _conditions = (f"{_col}={_value}" for _col, _value in col_values.items())
            where_stmt = " AND ".join(_conditions)

        order_by_stmt = ""
        if order_by:
            _order_by_stmts: list[str] = []
            for _item in order_by:
                if isinstance(_item, tuple):
                    _col, _direction = _item
                    cls.orm_check_cols(_col)
                    _order_by_stmts.append(f"{_col} {_direction}")
                else:
                    _order_by_stmts.append(_item)
            order_by_stmt = f"{','}.join(_order_by_stmts)"

        limit_stmt = f"LIMIT {limit} " if limit is not None else ""
        returning_stmt = (
            f"RETURNING {'*' if returning is True else returning} " if returning else ""
        )

        with StringIO() as buffer:
            buffer.write(delete_from_stmt)
            buffer.write(where_stmt)
            buffer.write(order_by_stmt)
            buffer.write(limit_stmt)
            buffer.write(returning_stmt)
            buffer.write(";")
            return buffer.getvalue()
