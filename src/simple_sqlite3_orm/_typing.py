"""Typing helpers definition."""

from __future__ import annotations

import sqlite3
from sqlite3 import Cursor, Row
from typing import Any, Callable

from typing_extensions import TypeAlias

from simple_sqlite3_orm._sqlite_spec import ORDER_DIRECTION

RowFactoryType: TypeAlias = (
    "Callable[[Cursor, Row | tuple[Any, ...] | Any], Any] | type[sqlite3.Row]"
)
"""Type hint for callable that can be used as sqlite3 row_factory."""

ConnectionFactoryType: TypeAlias = "Callable[[], sqlite3.Connection]"

ColsDefinitionWithDirection: TypeAlias = "tuple[str | tuple[str, ORDER_DIRECTION], ...]"
ColsDefinition: TypeAlias = "tuple[str, ...]"
