from __future__ import annotations

import sqlite3
from io import StringIO
from typing import Any, get_args

from pydantic import BaseModel
from typing_extensions import Self

from simple_sqlite3_orm.types import ConstrainLiteral, SQLiteDataType


def ConstrainRepr(*constrains: ConstrainLiteral | tuple[ConstrainLiteral, str]) -> str:
    """Compose column def and do some basic check over it."""
    constrain_kwds = get_args(ConstrainLiteral)
    with StringIO() as buffer:
        for _input_constrain in constrains:
            if isinstance(_input_constrain, tuple):
                _contrain, _option = _input_constrain
                assert _contrain in constrain_kwds
                buffer.write(f"{_contrain} {_option} ")
            else:
                assert _input_constrain in constrain_kwds
                buffer.write(f"{_input_constrain} ")
        return buffer.getvalue()


class ORMBase(BaseModel):
    @classmethod
    def orm_dump_column(cls, column_name: str) -> str:
        if not (field_info := cls.model_fields.get(column_name)):
            raise ValueError(f"{column_name=} not found")

        assert len(_meta := field_info.metadata) == 2
        _datatype_name, _constrain = _meta
        return f"{column_name} {_datatype_name} {_constrain}"

    @classmethod
    def get_create_table_stmt(
        cls,
        table_name: str,
        *,
        if_not_exists: bool = True,
        without_rowid: bool = False,
    ) -> str:
        with StringIO() as buffer:
            buffer.write(f"CREATE TABLE {table_name} ")
            if if_not_exists:
                buffer.write("IF NOT EXISTS ")
            buffer.write("( ")
            buffer.write(
                ",".join(cls.orm_dump_column(col_name) for col_name in cls.model_fields)
            )
            buffer.write(")")
            if without_rowid:
                buffer.write(" WITHOUT ROWID")
            buffer.write(";")
            return buffer.getvalue()

    @classmethod
    def get_create_index_stmt(
        cls,
        table_name: str,
        index_name: str,
        *cols: str,
    ) -> str:
        for _col in cols:
            if _col not in cls.model_fields:
                raise ValueError(f"{_col=} doesn't exist in {table_name}")
        _cols_spec = ", ".join(cols)
        return f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({_cols_spec})"

    @classmethod
    def row_factory(
        cls,
        _cursor: sqlite3.Cursor,
        _row: tuple[Any, ...] | sqlite3.Row,
    ) -> Self:
        """row_factory implement for used in sqlite3 connection."""
        _fields = [col[0] for col in _cursor.description]
        return cls.model_construct(**dict(zip(_fields, _row)))

    def orm_as_tuple(self) -> tuple[SQLiteDataType, ...]:
        """Dump self to a tuple of col values."""
        return tuple(getattr(self, _col) for _col in self.model_fields)
