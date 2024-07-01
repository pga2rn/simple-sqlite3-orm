from __future__ import annotations

import sqlite3
from io import StringIO
from typing import Any, Iterable, Literal, TypeVar

from pydantic import BaseModel
from pydantic.fields import FieldInfo
from typing_extensions import Self

from simple_sqlite3_orm._sqlite_spec import (
    INSERT_OR,
    ORDER_DIRECTION,
    SQLiteBuiltInFuncs,
    SQLiteStorageClass,
)
from simple_sqlite3_orm._utils import ConstrainRepr, TypeAffinityRepr


class TableSpec(BaseModel):
    """Define table as pydantic model, with specific APIs."""

    @classmethod
    def table_get_col_fieldinfo(cls, col: str) -> FieldInfo:
        """Check whether the <col> exists and returns the pydantic FieldInfo.

        Raises:
            ValueError on non-existed col.
        """
        if metadata := cls.model_fields.get(col):
            return metadata
        raise ValueError(f"{col} is not defined in {cls=}")

    @classmethod
    def table_check_cols(cls, cols: list[str]) -> None:
        """Ensure all cols in <cols> existed in the table definition.

        Raises:
            ValueError if any of col doesn't exist in the table.
        """
        for col in cols:
            if col not in cls.model_fields:
                raise ValueError(f"{col} is not defined in {cls=}")

    @classmethod
    def table_dump_column(cls, column_name: str) -> str:
        """Dump the column statement for table creation.

        Raises:
            ValueError on col doesn't exist or invalid col definition.
        """
        datatype_name, constrain = "", ""
        for metadata in cls.table_get_col_fieldinfo(column_name).metadata:
            if isinstance(metadata, TypeAffinityRepr):
                datatype_name = metadata
            elif isinstance(metadata, ConstrainRepr):
                constrain = metadata
        if not datatype_name:
            raise ValueError("data affinity must be set")
        return f"{column_name} {datatype_name} {constrain}".strip()

    @classmethod
    def table_create_stmt(
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

        cols_spec = ",".join(
            cls.table_dump_column(col_name) for col_name in cls.model_fields
        )
        return (
            f"CREATE {'TEMPORARY ' if temporary else ''}"
            f"{'IF NOT EXISTS ' if if_not_exists else ''}"
            f"TABLE {table_name} ({cols_spec}) "
            f"{'WITHOUT ROWID ' if without_rowid else ''}"
            f"{'STRICT ' if strict else ''};"
        )

    @classmethod
    def table_create_index_stmt(
        cls,
        table_name: str,
        index_name: str,
        index_cols: list[str | tuple[str, ORDER_DIRECTION]],
        if_not_exists: bool = False,
        unique: bool = False,
    ) -> str:
        """Get index create statement for this table spec class.

        Check https://www.sqlite.org/lang_createindex.html for more details.
        """
        assert index_cols, "at least one col should be specified for an index"

        _indexed_cols: list[str] = []
        for _input in index_cols:
            if isinstance(_input, tuple):
                _col, _order = _input
                cls.table_get_col_fieldinfo(_col)
                _indexed_cols.append(f"{_col} {_order}")
            else:
                _col = _input
                cls.table_get_col_fieldinfo(_col)
                _indexed_cols.append(_col)
        indexed_columns_stmt = f"({','.join(_indexed_cols)}) "

        return (
            f"CREATE {'UNIQUE' if unique else ''} INDEX "
            f"{'IF NOT EXISTS' if if_not_exists else ''} {index_name} "
            f"ON {table_name} {indexed_columns_stmt};"
        )

    @classmethod
    def table_row_factory(
        cls, _cursor: sqlite3.Cursor, _row: tuple[Any, ...] | sqlite3.Row
    ) -> Self:
        """row_factory implement for used in sqlite3 connection."""
        _fields = [col[0] for col in _cursor.description]
        return cls.model_validate(dict(zip(_fields, _row)))

    def table_dump_astuple(self, *cols: str) -> tuple[SQLiteStorageClass, ...]:
        """Dump self to a tuple of col values.

        The dumped tuple can be used to directly insert into the table.

        Args:
            *cols: which cols to export, if not specified, export all cols.

        Raises:
            ValueError if failed to serialize the model, wrapping underlying
                pydantic serialization error.

        Returns:
            A tuple of dumped col values from this row.
        """
        try:
            if not cols:
                return tuple(self.model_dump().values())
            return tuple(self.model_dump(include=set(cols)).values())
        except Exception as e:
            raise ValueError(f"failed to dump as tuple: {e!r}") from e

    @classmethod
    def table_insert_stmt(
        cls,
        insert_into: str,
        insert_cols: list[str] | None = None,
        or_option: INSERT_OR | None = None,
        *,
        insert_default: bool = False,
        returning: bool | str = False,
        retunring_cols: list[str] | None = None,
    ) -> str:
        """Get sql for inserting row(s) into <table_name>.

        Check https://www.sqlite.org/lang_insert.html for more details.
        """
        or_option_stmt = ""
        if or_option:
            or_option_stmt = f"OR {or_option.upper()}"
        insert_stmt = f"INSERT {or_option_stmt} INTO {insert_into} "

        if insert_cols:
            cls.table_check_cols(insert_cols)
            cols_specify_stmt = f"({','.join(insert_cols)}) "
            value_placeholder_stmt = f"VALUES ({','.join(['?'] * len(insert_cols))}) "
        else:
            cols_specify_stmt = ""
            value_placeholder_stmt = (
                f"VALUES ({','.join(['?'] * len(cls.model_fields))}) "
            )

        returning_stmt = ""
        if returning:
            returning_cols_stmt = ",".join(retunring_cols) if retunring_cols else "*"
            returning_stmt = f"RETURNING {returning_cols_stmt} "

        with StringIO() as buffer:
            buffer.write(insert_stmt)
            buffer.write(cols_specify_stmt)

            if insert_default:
                buffer.write("DEFAULT VALUES ")
            buffer.write(value_placeholder_stmt)

            buffer.write(returning_stmt)
            buffer.write(";")
            return buffer.getvalue()

    @classmethod
    def table_select_stmt(
        cls,
        select_from: str,
        select_cols: list[str] | Literal["*"] = "*",
        distinct: bool = False,
        function: SQLiteBuiltInFuncs | None = None,
        group_by: list[str] | None = None,
        order_by: list[str | tuple[str, ORDER_DIRECTION]] | None = None,
        limit: int | str | None = None,
        where: str | None = None,
        where_cols: list[str] | None = None,
    ) -> str:
        """Get sql for getting row(s) from <table_name>, optionally with
            where condition specified by <col_values>.

        Check https://www.sqlite.org/lang_select.html for more details.
        """
        select_target = "*"
        if isinstance(select_cols, list):
            cls.table_check_cols(select_cols)
            select_target = ",".join(select_cols)

        if function:
            select_target = f"{function}({select_target})"
        select_stmt = f"SELECT {'DISTINCT ' if distinct else ''}{select_target} "
        from_stmt = f"FROM {select_from} "

        where_stmt = ""
        if where:
            where_stmt = f"WHERE {where} "
        elif where_cols:
            _conditions: list[str] = []
            for _col in where_cols:
                if _col in cls.model_fields:
                    _conditions.append(f"{_col}=?")
            where_stmt = f"WHERE {' AND '.join(_conditions)} "

        group_by_stmt = f"GROUP BY {','.join(group_by)} " if group_by else ""

        order_by_stmt = ""
        if order_by:
            _order_by_stmts: list[str] = []
            for _item in order_by:
                if isinstance(_item, tuple):
                    _col, _direction = _item
                    cls.table_get_col_fieldinfo(_col)
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
    def table_delete_stmt(
        cls,
        delete_from: str,
        limit: int | str | None = None,
        order_by: Iterable[str | tuple[str, ORDER_DIRECTION]] | None = None,
        where: str | None = None,
        returning: bool | str | None = None,
        where_cols: list[str] | None = None,
    ) -> str:
        """Get sql for deleting row(s) from <table_name> with specifying col value(s).

        Check https://www.sqlite.org/lang_delete.html for more details.

        NOTE(20240311): DELETE operation without any condition(no WHERE statement) in
            WITHOUT_ROWID table will result in rowcount=0, see
            https://sqlite.org/forum/forumpost/07dedbf9a1 for more details.
            For python, python version < 3.10 will be affected by this bug.
            A quick workaround is to add any condition in where statement, even a dummy
            "WHERE 1=1" can resolve the above bug.
            I will not add this hack here, and user can add this hack according to their needs.
        """
        delete_from_stmt = f"DELETE FROM {delete_from} "

        where_stmt = ""
        if where:
            where_stmt = f"WHERE {where} "
        elif where_cols:
            cls.table_check_cols(where_cols)
            _conditions = (f"{_col}=?" for _col in where_cols)
            _where_cols_stmt = " AND ".join(_conditions)
            where_stmt = f"WHERE {_where_cols_stmt} "

        order_by_stmt = ""
        if order_by:
            _order_by_stmts: list[str] = []
            for _item in order_by:
                if isinstance(_item, tuple):
                    _col, _direction = _item
                    cls.table_get_col_fieldinfo(_col)
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


TableSpecType = TypeVar("TableSpecType", bound=TableSpec)
