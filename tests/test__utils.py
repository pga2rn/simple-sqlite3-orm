from __future__ import annotations

from typing import Optional

import pytest

from simple_sqlite3_orm import ConstrainRepr, TypeAffinityRepr
from simple_sqlite3_orm._sqlite_spec import SQLiteTypeAffinity
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
    ),
)
def test_typeafinityrepr(_in, expected):
    assert TypeAffinityRepr(_in) == expected


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
    assert ConstrainRepr(_in) == expected
