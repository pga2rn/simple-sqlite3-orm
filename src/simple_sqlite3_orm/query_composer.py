from __future__ import annotations

from functools import partialmethod
from io import StringIO
from typing import Generic, Iterable, Literal, Mapping, TypeVar

from typing_extensions import ParamSpec, Self

from simple_sqlite3_orm._sqlite_spec import OR_OPTIONS, ORDER_DIRECTION

RT = TypeVar("RT")
P = ParamSpec("P")
DefinedCols = TypeVar("DefinedCols", bound=str)

__all__ = [
    "ColumnSelector",
    "WhereStmtBuilder",
    "JoinStmtBuilder",
    "select",
    "insert",
    "update",
    "delete",
]


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
    def __init__(self) -> None:
        self._buffer = StringIO()
        self._query = None

    def _write(self, *_in: str, sep: str = " ", end_with: str = " ") -> Self:
        self._buffer.write(f"{sep.join(_in)}{end_with}")
        return self

    def _no_value_op(self, *, kw: str) -> Self:
        return self._write(kw)

    def _operators(self, _left: str, _right: str, *, op: str) -> Self:
        return self._write(_left, op, _right, sep="")

    def _append_single_expr(self, expr: str, *, kw: str) -> Self:
        return self._write(expr, kw)

    def _follow_single_expr(self, expr: str, *, kw: str) -> Self:
        return self._write(kw, expr)

    def add_raw_stmt(self, expr: str) -> Self:
        return self._write(expr)

    def finish_build(self, end_with: str = "") -> str:
        if self._query:
            return self._query

        try:
            self._write(end_with)
            self._query = res = self._buffer.getvalue().strip()
            return res
        finally:
            self._buffer.close()

    def getvalue(self) -> str:
        """Get the contents from the buffer without finish the build."""
        if self._query:
            return self._query
        return self._buffer.getvalue()

    @property
    def query(self) -> str:
        return self.finish_build()


class _FromStmtMixin(_BuilderBase):
    def from_(self, target: str) -> Self:
        return self._write("FROM", target)


class _WhereStmtMixin(_BuilderBase):
    def where(self, where_stmt: str | WhereStmtBuilder) -> Self:
        if isinstance(where_stmt, WhereStmtBuilder):
            where_stmt = where_stmt.query
        return self._write(where_stmt)


class WhereStmtBuilder(_BuilderBase):
    def __init__(self) -> None:
        super().__init__()
        self._write("WHERE")

    is_null = partialmethod[Self](_BuilderBase._append_single_expr, kw="IS NULL")
    is_not_null = partialmethod[Self](
        _BuilderBase._append_single_expr, kw="IS NOT NULL"
    )
    and_ = partialmethod[Self](_BuilderBase._no_value_op, kw="AND")
    or_ = partialmethod[Self](_BuilderBase._follow_single_expr, kw="OR")

    equal = partialmethod[Self](_BuilderBase._operators, op="=")
    not_equal = partialmethod[Self](_BuilderBase._operators, op="!=")
    greater = partialmethod[Self](_BuilderBase._operators, op=">")
    greater_equal = partialmethod[Self](_BuilderBase._operators, op=">=")
    less = partialmethod[Self](_BuilderBase._operators, op="<")
    less_equal = partialmethod[Self](_BuilderBase._operators, op="<=")


class JoinStmtBuilder(_BuilderBase):
    def __init__(
        self,
        join_target: str,
        join_type: Literal[
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL JOIN",
            "CROSS JOIN",
        ] = "JOIN",
    ):
        super().__init__()
        self._write(join_type, join_target)

    def as_(self, alias: str) -> Self:
        return self._write("AS", alias)

    def on(self, ref_col: str, target_col: str) -> Self:
        return self._write("ON", f"{ref_col}={target_col}")

    def using(self, *_cols: str) -> Self:
        return self._write("USING", f"({','.join(_cols)})", sep="")


class _JoinStmtMixin(_BuilderBase):
    def join(self, join_stmt: str | JoinStmtBuilder) -> Self:
        if isinstance(join_stmt, JoinStmtBuilder):
            join_stmt = join_stmt.query
        return self._write(join_stmt)


class _ReturningStmtMixin(_BuilderBase):
    def returning(self, _cols: list[str] | Literal["*"]) -> Self:
        return self._write("RETURNING", ",".join(_cols))


class _OrderByStmtMixin(_BuilderBase):
    def order_by(self, exprs: list[str | tuple[str, ORDER_DIRECTION]]) -> Self:
        return self._write(
            "ORDER BY",
            *(_elem if isinstance(_elem, str) else " ".join(_elem) for _elem in exprs),
        )


class _GroupByStmtMixin(_BuilderBase):
    def group_by(self, _cols: list[str]) -> Self:
        return self._write("GROUP BY", ",".join(_cols))


class _LimitStmtMixin(_BuilderBase):
    def limit(self, limit: int, offset_stmt: str | None = None) -> Self:
        self._write("LIMIT", str(limit))
        if offset_stmt:
            self._write(" ", offset_stmt)
        return self


class _InsertQueryBuilder(_ReturningStmtMixin, _BuilderBase):
    def __init__(self) -> None:
        super().__init__()
        self._write("INSERT")

    def or_(self, or_option: OR_OPTIONS) -> Self:
        return self._write("OR", or_option)

    default_value = partialmethod[Self](_BuilderBase._no_value_op, kw="DEFAULT VALUES")

    def into(self, table_name: str, _cols: Iterable[str]) -> Self:
        return self._write("INTO", table_name, "(", ",".join(_cols), ")")

    def values(self, placeholders: Iterable[str]) -> Self:
        return self._write("VALUES", "(", ",".join(placeholders), ")")


insert = _InsertQueryBuilder


class _SelectQueryBuilder(
    _LimitStmtMixin,
    _GroupByStmtMixin,
    _OrderByStmtMixin,
    _WhereStmtMixin,
    _JoinStmtMixin,
    _FromStmtMixin,
    _BuilderBase,
):
    def __init__(self, *_cols: str | Literal["*"]) -> None:
        super().__init__()
        self._write("SELECT", ",".join(_cols))

    def distinct(self) -> Self:
        return self._write("DISTINCT")


select = _SelectQueryBuilder


class _DeleteQueryBuilder(
    _LimitStmtMixin,
    _OrderByStmtMixin,
    _ReturningStmtMixin,
    _WhereStmtMixin,
    _FromStmtMixin,
    _BuilderBase,
):
    def __init__(self) -> None:
        super().__init__()
        self._write("DELETE")


delete = _DeleteQueryBuilder


class _UpdateQueryBuilder(
    _LimitStmtMixin,
    _OrderByStmtMixin,
    _ReturningStmtMixin,
    _WhereStmtMixin,
    _BuilderBase,
):
    def __init__(self, or_option: OR_OPTIONS | None = None) -> None:
        super().__init__()
        self._write("UPDATE")
        if or_option:
            self._write("OR", or_option)

    def set(self, named_placeholders: Mapping[str, str]) -> Self:
        return self._write("SET", ",".join(f"{k}=:{v}" for k, v in named_placeholders))


update = _UpdateQueryBuilder


# fmt: off
query = select("base.ft_regular.path", "base.ft_resource.digest").distinct().\
    from_("base.ft_regular").\
    join(JoinStmtBuilder("base.ft_resource").using("resource_id")).\
    join(
        JoinStmtBuilder("ft_resource")
        .as_("target_rs")
        .on("base.ft_resource.digest","target_rs.digest")
    ).\
    where(
        WhereStmtBuilder().not_equal("base.ft_resource.size", "0").and_().is_null("target_rs.contents")
    )
# fmt: on
query.finish_build()
print(query.query)
