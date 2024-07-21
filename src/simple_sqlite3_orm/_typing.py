from __future__ import annotations

from typing import Any, Callable

from typing_extensions import ParamSpec

P = ParamSpec("P")


def copy_callable_typehint(src: Callable[P, Any]):
    """This helper function return a decorator that can type hint the target
    function as the _source function.

    At runtime, this decorator actually does nothing, but just return the input function as it.
    But the returned function will have the same type hint as the source function in ide.
    It will not impact the runtime behavior of the decorated function.
    """

    def _decorator(target) -> Callable[P, Any]:
        target.__doc__ = src.__doc__
        return target

    return _decorator
