from __future__ import annotations

from typing import Any
from weakref import WeakValueDictionary

from simple_sqlite3_orm._table_spec import TableSpec, TableSpecType
from simple_sqlite3_orm._utils import GenericAlias

_parameterized_orm_cache: WeakValueDictionary[
    tuple[type[Any], type[TableSpec]],
    type[Any[TableSpec]],
] = WeakValueDictionary()


def class_getitem(cls, params: Any | type[Any] | type[TableSpecType]) -> Any:
    # just for convienience, passthrough anything that is not type[TableSpecType]
    #   to Generic's __class_getitem__ and return it.
    # Typically this is for subscript ORMBase with TypeVar or another Generic.
    if not (isinstance(params, type) and issubclass(params, TableSpec)):
        return super(cls).__class_getitem__(params)

    key = (cls, params)
    if _cached_type := _parameterized_orm_cache.get(key):
        return GenericAlias(_cached_type, params)

    new_parameterized_ormbase: type[Any] = type(
        f"{cls.__name__}[{params.__name__}]", (cls,), {}
    )
    new_parameterized_ormbase.orm_table_spec = params
    _parameterized_orm_cache[key] = new_parameterized_ormbase
    return GenericAlias(new_parameterized_ormbase, params)
