from __future__ import annotations

import sys
from io import StringIO
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from typing_extensions import ParamSpec

from simple_sqlite3_orm._sqlite_spec import (
    ConstrainLiteral,
    SQLiteTypeAffinity,
    SQLiteTypeAffinityLiteral,
)

P = ParamSpec("P")
RT = TypeVar("RT")

if TYPE_CHECKING:

    def lru_cache(_func: Callable[P, RT], /) -> Callable[P, RT]:
        """typeshed doesn't use ParamSpec for lru_cache typing currently."""
        raise NotImplementedError

else:
    from functools import lru_cache  # noqa: F401


if sys.version_info >= (3, 9):
    from types import GenericAlias  # noqa: F401
else:
    from typing import List

    if not TYPE_CHECKING:
        GenericAlias = type(List[int])
    else:

        class GenericAlias(type(List)):
            def __new__(
                cls, _type: type[Any], _params: type[Any] | tuple[type[Any], ...]
            ):
                """For type check only, typing the _GenericAlias as GenericAlias."""


def map_type(
    _in: type[Any] | SQLiteTypeAffinityLiteral | Any,
) -> SQLiteTypeAffinity | str:
    """Mapping python types to corresponding sqlite storage classes.

    Currently this function suports the following input:
    1. sqlite3 native types(and wrapped in Optional).
    2. Literal types.
    3. Enum types with str or int as data type.
    4. user defined affinity string, will be used as it.

    """
    if _in is None or _in is type(None):
        return SQLiteTypeAffinity.NULL

    if isinstance(_in, str):  # user-define type affinity, use as it
        return _in

    _origin = get_origin(_in)
    if _origin is Literal:
        return _map_from_literal(_in)
    if (
        _origin is Union
        and len(_args := get_args(_in)) == 2
        and _args[-1] is type(None)
    ):
        # Optional[X] is actually Union[X, type(None)]
        # after extract the actual types from Optional,
        #   do mapping from the beginning.
        return map_type(_args[0])
    if _origin is not None:
        raise TypeError(f"not one of Literal or Optional: {_in}")

    if not isinstance(_in, type):
        raise TypeError(f"expecting type or str object, get {type(_in)=}")
    return _map_from_type(_in)


def _map_from_literal(_in: Any) -> SQLiteTypeAffinity:
    """Support for literal of supported datatypes."""
    _first_literal, *_literals = get_args(_in)
    literal_type = type(_first_literal)

    if any(not isinstance(_literal, literal_type) for _literal in _literals):
        raise TypeError(f"mix types in literal is not allowed: {_in}")
    return _map_from_type(literal_type)


def _map_from_type(_in: type[Any]) -> SQLiteTypeAffinity:
    if issubclass(_in, int):  # NOTE: also include IntEnum
        return SQLiteTypeAffinity.INTEGER
    elif issubclass(_in, str):  # NOTE: also include StrEnum
        return SQLiteTypeAffinity.TEXT
    elif issubclass(_in, bytes):
        return SQLiteTypeAffinity.BLOB
    elif issubclass(_in, float):
        return SQLiteTypeAffinity.REAL
    raise TypeError(f"cannot map {_in} to any sqlite3 type affinity")


class TypeAffinityRepr:
    """Map python types to sqlite3 data types with type affinity.

    Currently supports:
    1. python sqlite3 lib supported native python types.
    2. StrEnum and IntEnum, will map to TEXT and INT accordingly.
    3. Optional types, will map against the args inside the Optional.
    4. Literal types, will map against the values type inside the Literal.

    Attrs:
        type_affinity (SQLiteTypeAffinity | str)
        origin (type[Any] | SQLiteTypeAffinityLiteral | Any)
    """

    def __init__(self, _in: type[Any] | SQLiteTypeAffinityLiteral | Any) -> None:
        self.type_affinity = map_type(_in)
        self.origin = _in

    def __str__(self) -> str:
        if isinstance(self.type_affinity, SQLiteTypeAffinity):
            return self.type_affinity.value
        return self.type_affinity

    def __repr__(self) -> str:  # pragma: no cover
        return f'<{self.__class__.__qualname__}: "{self}">'

    def __eq__(self, other: object) -> bool:  # pragma: no cover
        return (
            isinstance(other, self.__class__)
            and other.type_affinity == self.type_affinity
            and other.origin == self.origin
        )

    def __hash__(self) -> int:  # pragma: no cover
        return hash(self.origin)


class ConstrainRepr:
    """Helper class for composing full constrain statement string.

    For example, for constrain statement like the following:
        NOT NULL DEFAULT NULL CHECK (column IN (1, 2, 3))
    can be represented ConstrainRepr as follow:
        ConstrainRepr(
            "NOT NULL",
            ("DEFAULT", "NULL"),
            ("CHECK", r"(column IN (1, 2, 3))")
        )

    Attrs:
        constraints (set[str | tuple[str, str]]): a set of constrains.
    """

    def __init__(
        self, *params: ConstrainLiteral | tuple[ConstrainLiteral, str] | Any
    ) -> None:
        self.constraints = tuple(params)

    def __str__(self) -> str:
        with StringIO() as _buffer:
            for arg in self.constraints:
                if isinstance(arg, tuple):
                    _buffer.write(" ".join(arg))
                else:
                    _buffer.write(arg)
                _buffer.write(" ")
            return _buffer.getvalue().strip()

    def __repr__(self) -> str:  # pragma: no cover
        return f'<{self.__class__.__qualname__}: "{self}">'

    def __eq__(self, other: Any) -> bool:  # pragma: no cover
        return (
            isinstance(other, self.__class__) and other.constraints == self.constraints
        )

    def __hash__(self) -> int:  # pragma: no cover
        return hash(self.constraints)


def gen_sql_stmt(*components: str) -> str:
    """Combine each components into a single sql stmt."""
    with StringIO() as buffer:
        for comp in components:
            if not comp:
                continue
            buffer.write(" ")
            buffer.write(comp)
        buffer.write(";")
        return buffer.getvalue().strip()
