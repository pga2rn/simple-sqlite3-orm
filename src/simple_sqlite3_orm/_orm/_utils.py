from __future__ import annotations

from typing import Any
from weakref import WeakValueDictionary

from simple_sqlite3_orm._table_spec import TableSpec, TableSpecType
from simple_sqlite3_orm._utils import GenericAlias

_parameterized_orm_cache: WeakValueDictionary[
    tuple[type[Any], type[TableSpec]], type[Any[TableSpec]]
] = WeakValueDictionary()


def parameterized_class_getitem(
    cls, params: Any | type[Any] | type[TableSpecType] | tuple[type[Any], ...]
) -> Any:
    if not isinstance(params, type):
        raise TypeError(
            f"{cls.__name__} only allows to be parameterized with exactly one type, but get {params=}"
        )

    key = (cls, params)
    if _cached_type := _parameterized_orm_cache.get(key):
        return GenericAlias(_cached_type, params)

    # make a new type from the param
    if issubclass(params, TableSpec):
        new_parameterized_ormbase: type[Any] = type(
            f"{cls.__name__}[{params.__name__}]", (cls,), {}
        )
        new_parameterized_ormbase.orm_table_spec = params
        _parameterized_orm_cache[key] = new_parameterized_ormbase
        return GenericAlias(new_parameterized_ormbase, params)

    # just for convienience, for anything that is not type[TableSpecType],
    #   wrap it with GenericAlias.
    # actually we should only accept TypeVars or Generic, but that is not a very big problem,
    #   so just leave the current implementation as it.
    # typically this is for typing purpose.
    return GenericAlias(cls, params)
