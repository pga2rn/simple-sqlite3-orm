from __future__ import annotations

from abc import ABC
from typing import Any, Literal, TypeVar, Union

from typing_extensions import Annotated

# ------ sqlite3 data type ------ #
# ref: https://www.sqlite.org/datatype3.html


class _NULL_Type(ABC):
    """Singleton for NULL type."""

    def __new__(cls, *args: Any, **kwargs: Any) -> None:
        return None


_NULL_Type.register(type(None))


# NOTE: sqlite3 doesn't have bool type, python bool
#       will be converted to INTEGER 0 or 1 by py sqlite3
INTEGER_Type = Annotated[int, "INTEGER"]
TEXT_Type = Annotated[str, "TEXT"]
REAL_Type = Annotated[float, "REAL"]
BLOB_Type = Annotated[bytes, "BLOB"]
NULL_Type = Annotated[_NULL_Type, "NULL"]
NUMERIC_Type = Union[INTEGER_Type, TEXT_Type, REAL_Type, BLOB_Type, NULL_Type]

SQLiteDataType = Union[INTEGER_Type, TEXT_Type, REAL_Type, BLOB_Type, NULL_Type]
SQLiteDataTypes = TypeVar("SQLiteDataTypes", bound=SQLiteDataType)

DataTypeName = Literal["INTEGER", "TEXT", "REAL", "BLOB", "NULL"]

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
