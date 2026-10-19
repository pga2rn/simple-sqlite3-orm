from __future__ import annotations

import sys
from typing import Optional, Union

import pytest

from simple_sqlite3_orm._sqlite_spec import SQLiteTypeAffinity
from simple_sqlite3_orm._utils import ConstrainRepr, TypeAffinityRepr
from tests.sample_db._types import Choice123, ChoiceABC, SomeIntLiteral, SomeStrLiteral


@pytest.mark.parametrize(
    "_in, expected",
    (
        (Choice123, SQLiteTypeAffinity.INTEGER),
        (ChoiceABC, SQLiteTypeAffinity.TEXT),
        (str, SQLiteTypeAffinity.TEXT),
        (bytes, SQLiteTypeAffinity.BLOB),
        (int, SQLiteTypeAffinity.INTEGER),
        (float, SQLiteTypeAffinity.REAL),
        (None, SQLiteTypeAffinity.NULL),
        (SomeIntLiteral, SQLiteTypeAffinity.INTEGER),
        (SomeStrLiteral, SQLiteTypeAffinity.TEXT),
        (Optional[bytes], SQLiteTypeAffinity.BLOB),
        (Optional[Choice123], SQLiteTypeAffinity.INTEGER),
        (Optional[ChoiceABC], SQLiteTypeAffinity.TEXT),
        (Optional[SomeIntLiteral], SQLiteTypeAffinity.INTEGER),
        (Optional[SomeStrLiteral], SQLiteTypeAffinity.TEXT),
        # NoneType is not necessarily the last arg of the Union
        (Union[None, int], SQLiteTypeAffinity.INTEGER),
        (Union[None, Choice123], SQLiteTypeAffinity.INTEGER),
        (Union[None, SomeStrLiteral], SQLiteTypeAffinity.TEXT),
        # PEP 604 union syntax, only evaluable on Python 3.10+
        *(
            (
                (int | None, SQLiteTypeAffinity.INTEGER),
                (None | str, SQLiteTypeAffinity.TEXT),
                (bytes | None, SQLiteTypeAffinity.BLOB),
                (float | None, SQLiteTypeAffinity.REAL),
                (Choice123 | None, SQLiteTypeAffinity.INTEGER),
                (ChoiceABC | None, SQLiteTypeAffinity.TEXT),
                (SomeIntLiteral | None, SQLiteTypeAffinity.INTEGER),
                (SomeStrLiteral | None, SQLiteTypeAffinity.TEXT),
            )
            if sys.version_info >= (3, 10)
            else ()
        ),
    ),
)
def test_typeafinityrepr(_in, expected):
    _parsed = TypeAffinityRepr(_in)
    assert _parsed.type_affinity == expected
    assert str(_parsed) == expected.value


@pytest.mark.parametrize(
    "_in",
    (
        Union[int, str],
        Union[int, str, None],
        Optional[Union[int, str]],
        *((int | str, int | str | None) if sys.version_info >= (3, 10) else ()),
    ),
)
def test_typeafinityrepr_unsupported_union(_in):
    """Only a union of exactly one type and None (i.e. Optional[X]) is supported."""
    with pytest.raises(TypeError):
        TypeAffinityRepr(_in)


@pytest.mark.parametrize(
    "_in, expected",
    (
        (
            ConstrainRepr(
                "NOT NULL",
                ("DEFAULT", "1"),
                ("CHECK", r"(column IN (1,2,3))"),
            ),
            ("NOT NULL DEFAULT 1 CHECK (column IN (1,2,3))"),
        ),
    ),
)
def test_constrainrepr(_in, expected):
    assert f"{_in}" == expected
