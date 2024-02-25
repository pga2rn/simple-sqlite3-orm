from __future__ import annotations

import functools
import sys
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, TypeVar, get_args

from typing_extensions import ParamSpec

T = TypeVar("T")
P = ParamSpec("P")


def tp_cache(func: Callable[P, Any], *, max_size: Optional[int] = None):
    # basically the version from typing
    cached = functools.lru_cache(max_size)(func)

    @functools.wraps(func)
    def inner(*args: P.args, **kwds: P.kwargs):
        try:
            return cached(*args, **kwds)
        except TypeError:  # non-hashable type args preseneted
            pass  # All real errors (not unhashable args) are raised below.
        return func(*args, **kwds)

    return inner


# prepare GenericAlias and Union that behaves like in >= py3.10
# for older py version.

# The GenericAlias from typing module, note that this is different
# from the one in types.GenericAlias(used by parameterized built-in types).
tp_GenericAlias = type(Dict[str, int])


if sys.version_info >= (3, 10):
    from types import GenericAlias
    from typing import Union

elif sys.version_info >= (3, 9, 2):
    # NOTE: GenericAlias can be subclassed started from 3.9.2
    from types import GenericAlias as _std_GenericAlias

    # in py3.9, GenericAlias still don't support isinstance/issubclass check,
    # so we define a new GenericAlias based on the std one.
    class GenericAlias(_std_GenericAlias):
        def __instancecheck__(self, obj):
            if self.__origin__ is Union:
                return isinstance(obj, get_args(self))
            return isinstance(obj, self.__origin__)

        def __subclasscheck__(self, cls):
            if self.__origin__ is Union:
                return issubclass(cls, get_args(self))
            return issubclass(cls, self.__origin__)

    if TYPE_CHECKING:
        from typing import Union

    else:
        # at runtime, this Union supports isinstance/issubclass check
        class Union:
            @tp_cache
            def __class_getitem__(cls, params: Any):
                return GenericAlias(cls, params)

else:
    # In py3.8, types.GenericAlias is not yet avaible, while typing._GenericAlias is not subclassable,
    # we have to do some hack here to make it subclassable.
    # check typing._Final for how they prevent subclass and how we can bypass it.
    class _GenericAliasMeta(type):
        def __new__(
            cls,
            cls_name: str,
            bases: tuple[type[Any], ...],
            classdict: Dict[str, Any],
            **kwargs: Any,
        ):
            # inject _root=True into kwargs
            kwargs["_root"] = True
            return super().__new__(cls, cls_name, bases, classdict, **kwargs)

    class GenericAlias(tp_GenericAlias, metaclass=_GenericAliasMeta):
        # add isinstance/issubclass check support for our GenericAlias
        # currently only do special treatment to allow isinstace/issubclass
        # check against a Union.
        def __instancecheck__(self, obj: Any):
            if self.__origin__ is Union:
                return isinstance(obj, get_args(self))
            return isinstance(obj, self.__origin__)

        def __subclasscheck__(self, cls: Any):
            if self.__origin__ is Union:
                return issubclass(cls, get_args(self))
            return issubclass(cls, self.__origin__)

    if TYPE_CHECKING:
        from typing import Union

    else:
        # at runtime, we use our special version of Union
        class Union:
            @tp_cache
            def __class_getitem__(cls, params: Any):
                return GenericAlias(cls, params)
