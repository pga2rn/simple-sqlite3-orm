from __future__ import annotations

from functools import partial
from io import StringIO
from typing import Literal

from typing_extensions import Self

from simple_sqlite3_orm._sqlite_spec import (
    OR_OPTIONS,
)


class SimpleQueryComposer:
    """
    NOTE: this composer is NOT a strict and full implementation of sqlite3 syntax flow!
          NO checks are implemented during the composing of the query!
    """

    def __init__(self) -> None:
        self._buffer = StringIO()

    def _write(self, _in: str, end_with: str = " ") -> Self:
        self._buffer.write(f"{_in}{end_with}")
        return self

    def _follow_single_expr(self, expr: str, *, kw: str) -> Self:
        self._write(kw)
        return self._write(expr)

    def _follow_multiple_exprs(
        self,
        *exprs: str,
        kw: str,
        sep: Literal[" ", ","] = ",",
        wrap_with_parentheses: bool = False,
    ) -> Self:
        self._write(kw)
        if wrap_with_parentheses:
            return self._write(f"({sep.join(exprs)})")
        return self._write(f"{sep.join(exprs)}")

    def _append_single_expr(self, expr: str, *, kw: str) -> Self:
        self._write(expr)
        return self._write(kw)

    def _operators(self, _left: str, _right: str, *, kw: str) -> Self:
        return self._write(f"{_left} {kw} {_right}")

    select = partial(_follow_multiple_exprs, kw="SELECT")
    from_ = partial(_follow_single_expr, kw="FROM")

    insert = partial(_follow_single_expr, expr="", kw="INSERT")

    def or_options(self, or_options: OR_OPTIONS) -> Self:
        return self._write(f"OR {or_options}")

    into = partial(_follow_single_expr, kw="INTO")

    def join(
        self,
        join_target: str,
        join_type: Literal[
            "INNER JOIN",
            "JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL JOIN",
            "CROSS JOIN",
        ] = "JOIN",
    ) -> Self:
        self._write(join_type)
        return self._write(join_target)

    on = partial(_follow_single_expr, kw="ON")
    using = partial(_follow_multiple_exprs, kw="USING", wrap_with_parentheses=True)

    where = partial(_follow_single_expr, kw="WHERE")
    is_null = partial(_append_single_expr, kw="IS NULL")
    is_not_null = partial(_append_single_expr, kw="IS NOT NULL")
    and_ = partial(_follow_single_expr, kw="AND")
    or_ = partial(_follow_single_expr, kw="OR")
    equal = partial(_operators, kw="=")
    not_equal = partial(_operators, kw="!=")
    greater = partial(_operators, kw=">")
    greater_equal = partial(_operators, kw=">=")
    less = partial(_operators, kw="<")
    less_equal = partial(_operators, kw="<=")

    order_by = partial(_follow_multiple_exprs, kw="ORDER BY")
    limit = partial(_follow_single_expr, kw="LIMIT")
    returning = partial(_follow_multiple_exprs, kw="RETURNING")

    def finish_up_composing(self) -> str:
        try:
            self._write("", end_with=";")
            return self._buffer.getvalue()
        finally:
            self._buffer.close()
