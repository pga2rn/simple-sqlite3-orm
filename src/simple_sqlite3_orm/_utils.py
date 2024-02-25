from __future__ import annotations

from enum import Enum
from io import StringIO
from typing import Any, Literal, get_args, get_origin, Optional

from typing_extensions import Self

#
# ------ sqlite3 datatypes ------ #
#
# ref: https://www.sqlite.org/datatype3.html


class SQLiteStorageClass(str, Enum):
    NULL = "NULL"
    INTEGER = "INTEGER"
    REAL = "REAL"
    TEXT = "TEXT"
    BLOB = "BLOB"


SQLiteStorageClassLiteral = Literal["NULL", "INTEGER", "REAL", "TEXT", "BLOB"]


class SQLiteTypeAffinity(str, Enum):
    TEXT = "TEXT"
    NUMERIC = "NUMERIC"
    INTEGER = "INTEGER"
    REAL = "REAL"
    BLOB = "BLOB"


SQLiteTypeAffinityLiteral = Literal["TEXT", "NUMERIC", "INTEGER", "REAL", "BLOB"]


class TypeAffinityRepr(str):
    def __new__(cls, _in: type[Any] | SQLiteTypeAffinityLiteral | str | Any) -> Self:
        """Mapping python types to corresponding sqlite storage classes."""
        if isinstance(_in, str):  # user-define type affinity, use as it
            return str.__new__(cls, _in)

        if _origin := get_origin(_in):
            if _origin is Literal:
                return cls._map_from_literal(_in)
            if _origin is Optional:
                return cls._map_from_type(get_args(_origin)[0])
            raise TypeError(f"not one of Literal or Optional: {_in}")

        if not isinstance(_in, type):
            raise TypeError(f"expecting type or str object, get {type(_in)=}")
        return cls._map_from_type(_in)

    @classmethod
    def _map_from_literal(cls, _in: Any) -> Self:
        """Support for literal of supported datatypes."""
        _first_literal, *_literals = get_args(_in)
        literal_type = type(_first_literal)

        if any(not isinstance(_literal, literal_type) for _literal in _literals):
            raise TypeError(f"mix types in literal is not allowed: {_in}")
        return cls._map_from_type(literal_type)

    @classmethod
    def _map_from_type(cls, _in: type[Any]) -> Self:
        if issubclass(_in, int):  # NOTE: also include IntEnum
            return str.__new__(cls, SQLiteTypeAffinity.INTEGER)
        elif issubclass(_in, str):  # NOTE: also include StrEnum
            return str.__new__(cls, SQLiteTypeAffinity.TEXT)
        elif issubclass(_in, bytes):
            return str.__new__(cls, SQLiteTypeAffinity.BLOB)
        elif issubclass(_in, float):
            return str.__new__(cls, SQLiteTypeAffinity.REAL)
        raise TypeError(f"cannot map {_in} to any sqlite3 type affinity")


#
# ------ contrain keywords ------ #
#
# ref: https://www.sqlite.org/lang_createtable.html
ConstrainLiteral = Literal[
    "PRIMARY KEY",
    "NOT NULL",
    "UNIQUE",
    "CHECK",
    "DEFAULT",
    "COLLATE",
    "REFERENCES",
    "GENERATED ALWAYS AS",
    "AS",
]


class ConstrainRepr(str):
    def __new__(cls, *args: ConstrainLiteral | tuple[ConstrainLiteral, str]) -> Self:
        with StringIO() as _buffer:
            for arg in args:
                if isinstance(arg, tuple):
                    _buffer.write(" ".join(arg))
                else:
                    _buffer.write(arg)
                _buffer.write(" ")
            return str.__new__(cls, _buffer.getvalue().strip())
