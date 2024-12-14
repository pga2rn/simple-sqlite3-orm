from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Iterable, Optional

import pytest
from typing_extensions import Annotated

from simple_sqlite3_orm import ConstrainRepr, TableSpec, TypeAffinityRepr


class SimpleTableForTest(TableSpec):
    id: Annotated[
        int,
        TypeAffinityRepr(int),
        ConstrainRepr("PRIMARY KEY"),
    ]

    id_str: Annotated[
        str,
        TypeAffinityRepr(str),
        ConstrainRepr("NOT NULL"),
    ]

    extra: Annotated[
        Optional[float],
        TypeAffinityRepr(float),
    ] = None


@pytest.mark.parametrize(
    "_in, _validate, _expected",
    (
        ([1, "1", 1.0], True, SimpleTableForTest(id=1, id_str="1", extra=1.0)),
        ([1, "1", 1.0], False, SimpleTableForTest(id=1, id_str="1", extra=1.0)),
    ),
)
def test_table_from_tuple(
    _in: Iterable[Any], _validate: bool, _expected: SimpleTableForTest
):
    assert (
        SimpleTableForTest.table_from_tuple(_in, with_validation=_validate) == _expected
    )


@pytest.mark.parametrize(
    "_in, _validate, _expected",
    (
        (
            {"id": 1, "id_str": "1", "extra": 1.0},
            True,
            SimpleTableForTest(id=1, id_str="1", extra=1.0),
        ),
        (
            {"id": 1, "id_str": "1", "extra": 1.0},
            False,
            SimpleTableForTest(id=1, id_str="1", extra=1.0),
        ),
    ),
)
def test_table_from_dict(
    _in: Mapping[str, Any], _validate: bool, _expected: SimpleTableForTest
):
    assert (
        SimpleTableForTest.table_from_dict(_in, with_validation=_validate) == _expected
    )


@pytest.mark.parametrize(
    "_in, _expected",
    (
        (
            SimpleTableForTest(id=1, id_str="1", extra=1.0),
            {"id": 1, "id_str": "1", "extra": 1.0},
        ),
        (
            SimpleTableForTest(id=1, id_str="1", extra=1.0),
            {"id": 1, "extra": 1.0},
        ),
    ),
)
def test_table_dump_asdict(_in: SimpleTableForTest, _expected: dict[str, Any]):
    assert _in.table_dump_asdict(*_expected) == _expected


@pytest.mark.parametrize(
    "_in, _cols, _expected",
    (
        (
            SimpleTableForTest(id=1, id_str="1", extra=1.0),
            ["id", "id_str", "extra"],
            (1, "1", 1.0),
        ),
        (
            SimpleTableForTest(id=1, id_str="1", extra=1.0),
            ["extra"],
            (1.0,),
        ),
    ),
)
def test_table_dump_astuple(
    _in: SimpleTableForTest, _cols: tuple[str, ...], _expected: tuple[Any, ...]
):
    assert _in.table_dump_astuple(*_cols) == _expected
