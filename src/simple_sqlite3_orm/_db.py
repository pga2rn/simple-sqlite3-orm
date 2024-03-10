"""sqlite3 specific types and helpers."""

from __future__ import annotations

from enum import Enum
from typing import Literal

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


#
# ------ built-in functions ------ #
#
# Check https://www.sqlite.org/lang_corefunc.html for more details.
SQLiteBuiltInFuncs = Literal[
    "abs",
    "changes",
    "char",
    "coalesce",
    "concat",
    "concat_ws",
    "format",
    "glob",
    "hex",
    "ifnull",
    "iif",
    "instr",
    "last_insert_rowid",
    "length",
    "like",
    "like",
    "likelihood",
    "likely",
    "load_extension",
    "load_extension",
    "lower",
    "ltrim",
    "ltrim",
    "max",
    "min",
    "nullif",
    "octet_length",
    "printf",
    "quote",
    "random",
    "randomblob",
    "replace",
    "round",
    "round",
    "rtrim",
    "rtrim",
    "sign",
    "soundex",
    "sqlite_compileoption_get",
    "sqlite_compileoption_used",
    "sqlite_offset",
    "sqlite_source_id",
    "sqlite_version",
    "substr",
    "substr",
    "substring",
    "substring",
    "total_changes",
    "trim",
    "trim",
    "typeof",
    "unhex",
    "unhex",
    "unicode",
    "unlikely",
    "upper",
    "zeroblob",
]


INSERT_OR = Literal["abort", "fail", "ignore", "replace", "rollback"]
ORDER_DIRECTION = Literal["ASC", "DESC"]
