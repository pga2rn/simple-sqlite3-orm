from __future__ import annotations

import sqlite3
from io import StringIO
from typing import Any, Literal, Optional

from pydantic import BaseModel
from typing_extensions import Self

from simple_sqlite3_orm._utils import (
    ConstrainRepr,
    SQLiteStorageClass,
    TypeAffinityRepr,
)


class ORMBase(BaseModel):
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
        schema_name: Optional[str] = None,
        strict: bool = False,
        temporary: bool = False,
        without_rowid: bool = False,
    ) -> str:
        """Get create table statement for this table spec class.

        Check https://www.sqlite.org/lang_createtable.html for more details.
        """
        with StringIO() as buffer:
            buffer.write("CREATE ")
            if temporary:
                buffer.write("TEMPORARY ")
            if if_not_exists:
                buffer.write("IF NOT EXISTS ")
            buffer.write("TABLE ")

            _table_name_stmt = f"{table_name}"
            if schema_name:
                _table_name_stmt = f"{schema_name}.{table_name}"
            buffer.write(f"{_table_name_stmt} ")

            buffer.write("( ")
            buffer.write(
                ", ".join(
                    cls.orm_dump_column(col_name) for col_name in cls.model_fields
                )
            )
            buffer.write(") ")

            _table_options: list[str] = []
            if without_rowid:
                _table_options.append("WITHOUT ROWID")
            if strict:
                _table_options.append("STRICT")
            buffer.write(",".join(_table_options))

            buffer.write(";")
            return buffer.getvalue()

    @classmethod
    def simple_create_index_stmt(
        cls,
        table_name: str,
        index_name: str,
        *cols: str | tuple[str, Literal["ASC", "DESC"]],
        if_not_exists: bool = False,
        schema_name: Optional[str] = None,
        unique: bool = False,
    ) -> str:
        """Get index create statement for this table spec class.

        Check https://www.sqlite.org/lang_createindex.html for more details.
        """
        assert cols, "at least one col should be specified for an index"

        _indexed_col_stmts: list[str] = []
        for _input in cols:
            if isinstance(_input, tuple):
                _col, _order = _input
                assert _col in cls.model_fields, f"{_col=} is not a valid column"
                _indexed_col_stmts.append(f"{_col} {_order}")
            else:
                _col = _input
                assert _col in cls.model_fields, f"{_col=} is not a valid column"
                _indexed_col_stmts.append(_col)
        _indexed_columns_stmt = ", ".join(_indexed_col_stmts)

        with StringIO() as buffer:
            buffer.write("CREATE ")
            if unique:
                buffer.write("UNIQUE ")
            buffer.write("INDEX ")
            if if_not_exists:
                buffer.write("IF NOT EXISTS ")

            _index_name_stmt = f"{index_name}"
            if schema_name:
                _index_name_stmt = f"{schema_name}.{index_name}"
            buffer.write(f"{_index_name_stmt} ")

            buffer.write(f"ON {table_name} ")
            buffer.write(f"({_indexed_columns_stmt});")
            return buffer.getvalue()

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

        _cols = set(cols)
        _res: list[str] = []
        for _col in self.model_fields:
            if _col in _cols:
                _res.append(_col)
        return tuple(getattr(self, _col) for _col in _res)

    @classmethod
    def simple_insert_entry_stmt(
        cls, table_name: str, *cols: str, or_replace: bool = False
    ) -> str:
        """Get sql for inserting row(s) into <table_name>.

        Args:
            table_name: table to insert to.
            *cols: which cols will be assigned values, non-assigned col will get default
                value assigned(if no DEFAULT is defined, NULL will be assigned).
                if cols is empty, by default expecting all cols to be assigned.

        Returns:
            SQL statement like the following:
                INSERT [OR REPLACE] INTO <table_name>
                    [(<col_1>[,<col_2>, ...])] VALUES
                    (?[,?,...]);
        """
        with StringIO() as buffer:
            buffer.write("INSERT ")
            if or_replace:
                buffer.write("OR REPLACE ")
            buffer.write(f"INTO {table_name} ")

            if cols:
                _cols_set = set(cols)
                _cols_list: list[str] = []  # preserve input order
                for _col in cls.model_fields:
                    if _col in _cols_set:
                        _cols_list.append(_col)
                buffer.write("(")
                buffer.write(",".join(_cols_list))
                buffer.write(")")

                num_of_placeholder = len(cols)
            else:
                num_of_placeholder = len(cls.model_fields)

            buffer.write(" VALUES (")
            buffer.write(",".join(["?"] * num_of_placeholder))
            buffer.write(");")
            return buffer.getvalue()

    @classmethod
    def simple_select_entry_stmt(cls, table_name: str, **col_values: Any) -> str:
        """Get sql for getting row(s) from <table_name>.

        Args:
            table_name: table to select from.
            **col_values: WHERE condition to locate the target row(s).

        Returns:
            SQL statement like the following:
                SELECT * FROM <table_name>
                    WHERE <col_1>=<col_values[col_1]>
                        [AND <col_2>=<col_values[col_2]>[AND ...]]
        """
        with StringIO() as buffer:
            buffer.write(f"SELECT * FROM {table_name}")
            if col_values:
                _conditions: list[str] = []
                for _col, _value in col_values.items():
                    if _col not in cls.model_fields:
                        continue
                    _conditions.append(f"{_col}={_value}")

                buffer.write("WHERE ")
                buffer.write(" AND ".join(_conditions))
            buffer.write(";")
            return buffer.getvalue()

    @classmethod
    def simple_delete_entry_stmt(
        cls,
        table_name: str,
        limit: int = -1,
        order_by: str = "",
        **col_values: Any,
    ) -> str:
        """Get sql for deleting row(s) from <table_name> with specifying col value(s).

        Args:
            table_name: table to delete from.
            limit: will be appended to LIMIT keyword.
            order_by: will be appended to ORDER BY keyword.
            **col_values: WHERE condition to locate the target row(s).

        Returns:
            SQL statement like the following:
                DELETE FROM <table_name>
                    WHERE <col_1>=<col_values[col_1]>
                        [AND <col_2>=<col_values[col_2]>[AND ...]]
                    [ORDER BY <order_by>]
                    [LIMIT <limit>]
        """
        with StringIO() as buffer:
            buffer.write(f"DELETE FROM {table_name}")
            if col_values:
                _conditions: list[str] = []
                for _col, _value in col_values.items():
                    if _col not in cls.model_fields:
                        continue
                    _conditions.append(f"{_col}={_value}")

                buffer.write("WHERE ")
                buffer.write(" AND ".join(_conditions))
            if order_by:
                buffer.write(f" ORDER BY {order_by} ")
            if limit > 0:
                buffer.write(f" LIMIT {limit} ")
            buffer.write(";")
            return buffer.getvalue()
