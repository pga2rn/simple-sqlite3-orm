from __future__ import annotations

from io import StringIO
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Optional,
    TypeVar,
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
            return str.__new__(cls, SQLiteTypeAffinity.INTEGER.value)
        elif issubclass(_in, str):  # NOTE: also include StrEnum
            return str.__new__(cls, SQLiteTypeAffinity.TEXT.value)
        elif issubclass(_in, bytes):
            return str.__new__(cls, SQLiteTypeAffinity.BLOB.value)
        elif issubclass(_in, float):
            return str.__new__(cls, SQLiteTypeAffinity.REAL.value)
        raise TypeError(f"cannot map {_in} to any sqlite3 type affinity")


class ConstrainRepr(str):
    def __new__(
        cls, *args: ConstrainLiteral | tuple[ConstrainLiteral, str] | str
    ) -> Self:
        with StringIO() as _buffer:
            for arg in args:
                if isinstance(arg, tuple):
                    _buffer.write(" ".join(arg))
                else:
                    _buffer.write(arg)
                _buffer.write(" ")
            return str.__new__(cls, _buffer.getvalue().strip())
