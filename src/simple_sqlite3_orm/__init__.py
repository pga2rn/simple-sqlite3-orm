from __future__ import annotations

import sqlite3
from io import StringIO
from typing import Any

from pydantic import BaseModel
from typing_extensions import Self

from simple_sqlite3_orm._utils import (
    ConstrainRepr,
    SQLiteStorageClass,
    SQLiteStorageClassLiteral,
    SQLiteTypeAffinity,
    SQLiteTypeAffinityLiteral,
    TypeAffinityRepr,
)

__all__ = (
    "SQLiteStorageClass",
    "SQLiteStorageClassLiteral",
    "SQLiteTypeAffinity",
    "SQLiteTypeAffinityLiteral",
    "TypeAffinityRepr",
    "ConstrainRepr",
    "ORMBase",
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
        if_not_exists: bool = True,
        without_rowid: bool = False,
    ) -> str:
        """Get create table statement for this table spec class."""
        with StringIO() as buffer:
            buffer.write("CREATE TABLE ")
            if if_not_exists:
                buffer.write("IF NOT EXISTS ")
            buffer.write(f"{table_name} ( ")
            buffer.write(
                ", ".join(
                    cls.orm_dump_column(col_name) for col_name in cls.model_fields
                )
            )
            buffer.write(")")
            if without_rowid:
                buffer.write(" WITHOUT ROWID")
            buffer.write(";")
            return buffer.getvalue()

    @classmethod
    def simple_create_index_stmt(
        cls,
        table_name: str,
        index_name: str,
        *cols: str,
    ) -> str:
        """Get index create statement fro this table spec class."""
        for _col in cols:
            if _col not in cls.model_fields:
                raise ValueError(f"{_col=} doesn't exist in {table_name}")
        _cols_spec = ", ".join(cols)
        return f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({_cols_spec})"

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

