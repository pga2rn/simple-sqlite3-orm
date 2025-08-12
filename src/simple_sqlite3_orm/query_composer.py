from __future__ import annotations

from functools import partial
from io import StringIO
from typing import Generic, Literal, TypeVar

from typing_extensions import Self

from simple_sqlite3_orm._sqlite_spec import OR_OPTIONS, ORDER_DIRECTION

DefinedCols = TypeVar("DefinedCols", bound=str)


class ColumnSelector(Generic[DefinedCols]):
    @staticmethod
    def check(_col: DefinedCols) -> DefinedCols:
        return _col

    @staticmethod
    def get_cols_stmt(*_cols: DefinedCols, with_parenthesis: bool = False) -> str:
        if len(_cols) == 1:
            res = _cols[0]
        else:
            res = ",".join(_cols)

        if with_parenthesis:
            return f"({res})"
        return res


class _BuilderBase:
    def __init__(self, _initial: str = "") -> None:
        self._buffer = StringIO(_initial)
        self._res = None

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

    def _operators(self, _col: str, *, op: str) -> Self:
        return self._write(f"{_col} {op} :{_col}")

    def finish_up_composing(self, end_with: str) -> str:
        try:
            self._write("", end_with=end_with)
            self._res = res = self._buffer.getvalue()
            return res
        finally:
            self._buffer.close()

    def getvalue(self) -> str:
        if self._res:
            return self._res
        return self._buffer.getvalue()


class WhereStmtBuilder(_BuilderBase):
    def __init__(self) -> None:
        super().__init__("WHERE")

    is_null = partial(_BuilderBase._append_single_expr, kw="IS NULL")
    is_not_null = partial(_BuilderBase._append_single_expr, kw="IS NOT NULL")
    and_ = partial(_BuilderBase._follow_single_expr, kw="AND")
    or_ = partial(_BuilderBase._follow_single_expr, kw="OR")

    equal = partial(_BuilderBase._operators, op="=")
    not_equal = partial(_BuilderBase._operators, op="!=")
    greater = partial(_BuilderBase._operators, op=">")
    greater_equal = partial(_BuilderBase._operators, op=">=")
    less = partial(_BuilderBase._operators, op="<")
    less_equal = partial(_BuilderBase._operators, op="<=")


class JoinStmtBuilder(_BuilderBase):
    def __init__(
        self,
        join_type: Literal[
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL JOIN",
            "CROSS JOIN",
        ] = "JOIN",
    ):
        super().__init__(join_type)

    on = partial(_BuilderBase._follow_single_expr, kw="ON")
    using = partial(
        _BuilderBase._follow_multiple_exprs, kw="USING", wrap_with_parentheses=True
    )


class SimpleQueryBuilder(_BuilderBase):
    """
    NOTE: this composer is NOT a strict and full implementation of sqlite3 syntax flow!
          NO checks are implemented during the composing of the query!
    """

    @classmethod
    def _init(cls, *, kw: str) -> Self:
        return cls(kw)

    #
    # --- entry points --- #
    #
    # fmt: off
    insert = partial(_init, kw="INSERT")
    def or_options(self, or_options: OR_OPTIONS) -> Self:
        return self._write(f"OR {or_options}")
    into = partial(_BuilderBase._follow_single_expr, kw="INTO")
    # fmt: on

    select = partial(_init, kw="SELECT")
    update = partial(_init, kw="UPDATE")
    delete = partial(_init, kw="DELETE")

    # -------------------- #

    from_ = partial(_BuilderBase._follow_single_expr, kw="FROM")

    def join(self, _join_builder: JoinStmtBuilder) -> Self:
        return self._write(_join_builder.getvalue())

    def where(self, _where_builder: WhereStmtBuilder) -> Self:
        return self._write(_where_builder.getvalue())

    def order_by(self, *expr: str | tuple[str, ORDER_DIRECTION]):
        return self._follow_multiple_exprs(
            *(_elem if isinstance(_elem, str) else " ".join(_elem) for _elem in expr),
            kw="ORDER BY",
        )

    limit = partial(_BuilderBase._follow_single_expr, kw="LIMIT")
    returning = partial(_BuilderBase._follow_multiple_exprs, kw="RETURNING")
