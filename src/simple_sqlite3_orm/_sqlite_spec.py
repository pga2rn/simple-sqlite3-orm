"""sqlite3 specific types and helpers."""

from __future__ import annotations

from enum import Enum
from typing import Literal, Union

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
    NULL = "NULL"


SQLiteTypeAffinityLiteral = Literal["TEXT", "NUMERIC", "INTEGER", "REAL", "BLOB"]

#
# ------ contrain keywords ------ #
#
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
"""Ref: https://www.sqlite.org/lang_createtable.html"""


#
# ------ built-in functions ------ #
#
SQLiteBuiltInScalarFuncs = Literal[
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
"""Ref: https://www.sqlite.org/lang_corefunc.html"""

SQLiteBuiltInAggregateFuncs = Literal[
    "avg",
    "count",
    "count",
    "group_concat",
    "group_concat",
    "max",
    "min",
    "string_agg",
    "sum",
]
"""Ref: https://www.sqlite.org/lang_aggfunc.html"""

SQLiteBuiltInDateTimeFuncs = Literal[
    "data", "time", "datetime", "julianday", "unixepoch", "strftime", "timediff"
]
"""Ref: https://www.sqlite.org/lang_datefunc.html"""

SQLiteBuiltInMathFuncs = Literal[
    "acos",
    "acosh",
    "asin",
    "asinh",
    "atan",
    "atan2",
    "atanh",
    "ceil",
    "ceiling",
    "cos",
    "cosh",
    "degrees",
    "exp",
    "floor",
    "ln",
    "log",
    "log",
    "log10",
    "log2",
    "mod",
    "pi",
    "pow",
    "power",
    "radians",
    "sin",
    "sinh",
    "sqrt",
    "tan",
    "tanh",
    "trunc",
]
"""Ref: https://www.sqlite.org/lang_mathfunc.html"""

SQLiteBuiltInJSONFuncs = Literal[
    "json",
    "jsonb",
    "json_array",
    "jsonb_array",
    "json_array_length",
    "json_error_position",
    "json_extract",
    "jsonb_extract",
    "json_insert",
    "jsonb_insert",
    "json_object",
    "jsonb_object",
    "json_patch",
    "jsonb_patch",
    "json_remove",
    "jsonb_remove",
    "json_replace",
    "jsonb_replace",
    "json_set",
    "jsonb_set",
    "json_type",
    "json_valid",
    "json_quote",
    "json_group_array",
    "jsonb_group_array",
    "json_group_object",
    "jsonb_group_object",
    "json_each",
    "json_tree",
]
"""Ref: https://www.sqlite.org/json1.html"""

SQLiteBuiltInFuncs = Union[
    SQLiteBuiltInAggregateFuncs,
    SQLiteBuiltInDateTimeFuncs,
    SQLiteBuiltInJSONFuncs,
    SQLiteBuiltInMathFuncs,
    SQLiteBuiltInScalarFuncs,
]


#
# ------ other helper literals ------ #
#

INSERT_OR = Literal["abort", "fail", "ignore", "replace", "rollback"]
ORDER_DIRECTION = Literal["ASC", "DESC"]

CONDITION_OPERATORS = Literal[
    "AND",
    "OR",
    "IS",
    "NULL",
    "IS NULL",
    "IS NOT NULL",
    "NOT",
    "MATCH",
    "LIKE",
    "BETWEEN",
    "REGEXP",
    "GLOB",
    "IS DISTINCT FROM",
    "IS NOT DISTINCT FROM",
]
"""Ref https://www.sqlite.org/lang_expr.html"""

COMPARE_OPERATORS = Literal["=", "==", "!=", "<>", ">=", "<="]
