from __future__ import annotations

import sqlite3
from io import StringIO
from typing import Any, Iterable, Literal, Optional

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

        _input_cols = set(cols)
        return tuple(
            getattr(self, _col)
            for _col in filter(lambda x: x in _input_cols, self.model_fields)
        )

    @classmethod
    def simple_insert_entry_stmt(
        cls,
        insert_into: str,
        *cols: str,
        insert_default: bool = False,
        insert_select: Optional[str] = None,
        or_option: Optional[
            Literal["abort", "fail", "ignore", "replace", "rollback"]
        ] = None,
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
                buffer.write(f"DEFAULT VALUES {returning_stmt} ")
            if insert_select:
                buffer.write(f"{insert_select} ")
            else:
                buffer.write(values_specify_stmt)

            buffer.write(f"{returning_stmt};")
            return buffer.getvalue()

    @classmethod
    def simple_select_entry_stmt(
        cls,
        select_from: str,
        /,
        *select_cols: str,
        distinct: bool = False,
        function: Optional[str] = None,
        group_by: Optional[Iterable[str]] = None,
        order_by: Optional[Iterable[str]] = None,
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
        select_from_stmt = f"FROM {select_from} "

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
            buffer.write(select_from_stmt)
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
        order_by: Optional[Iterable[str | tuple[str, Literal["ASC", "DESC"]]]] = None,
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
