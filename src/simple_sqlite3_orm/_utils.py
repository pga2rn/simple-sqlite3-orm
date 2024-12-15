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

from typing_extensions import ParamSpec, Self

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
    from types import GenericAlias
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


class TypeAffinityRepr(str):
    """Map python types to sqlite3 data types with type affinity.

    Currently supports:
    1. python sqlite3 lib supported native python types.
    2. StrEnum and IntEnum, will map to TEXT and INT accordingly.
    3. Optional types, will map against the args inside the Optional.
    4. Literal types, will map against the values type inside the Literal.
    """

    def __new__(cls, _in: type[Any] | SQLiteTypeAffinityLiteral | Any) -> Self:
        """Mapping python types to corresponding sqlite storage classes."""
        if _in is None or _in is type(None):
            return str.__new__(cls, SQLiteTypeAffinity.NULL.value)

        if isinstance(_in, str):  # user-define type affinity, use as it
            return str.__new__(cls, _in)

        _origin = get_origin(_in)
        if _origin is Literal:
            return cls._map_from_literal(_in)
        if (
            _origin is Union
            and len(_args := get_args(_in)) == 2
            and _args[-1] is type(None)
        ):
            # Optional[X] is actually Union[X, type(None)]
            return cls._map_from_type(_args[0])
        if _origin is not None:
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
            return str.__new__(cls, SQLiteTypeAffinity.INTEGER.value)
        elif issubclass(_in, str):  # NOTE: also include StrEnum
            return str.__new__(cls, SQLiteTypeAffinity.TEXT.value)
        elif issubclass(_in, bytes):
            return str.__new__(cls, SQLiteTypeAffinity.BLOB.value)
        elif issubclass(_in, float):
            return str.__new__(cls, SQLiteTypeAffinity.REAL.value)
        raise TypeError(f"cannot map {_in} to any sqlite3 type affinity")


class ConstrainRepr(str):
    """Helper class for composing full constrain statement string.

    For example, for constrain statement like the following:
        NOT NULL DEFAULT NULL CHECK (column IN (1, 2, 3))
    can be represented ConstrainRepr as follow:
        ConstrainRepr(
            "NOT NULL",
            ("DEFAULT", "NULL"),
            ("CHECK", r"(column IN (1, 2, 3))")
        )
    """

    def __new__(
        cls, *args: ConstrainLiteral | tuple[ConstrainLiteral, str] | Any
    ) -> Self:
        with StringIO() as _buffer:
            for arg in args:
                if isinstance(arg, tuple):
                    _buffer.write(" ".join(arg))
                else:
                    _buffer.write(arg)
                _buffer.write(" ")
            return str.__new__(cls, _buffer.getvalue().strip())


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
