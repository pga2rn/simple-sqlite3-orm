"""A simple table example for testing."""

from __future__ import annotations

from typing import Optional

from typing_extensions import Annotated

from simple_sqlite3_orm import (
    ConstrainRepr,
    DatetimeISO8601,
    DatetimeUnixTimestamp,
    DatetimeUnixTimestampInt,
    TableSpec,
    TypeAffinityRepr,
)
from tests.sample_db._types import (
    Choice123,
    ChoiceABC,
    Mystr,
    SomeIntLiteral,
    SomeStrLiteral,
)


class SampleTable(TableSpec):
    """This sample table contains as much different types of fields as possible."""

    # ------ datetime related ------ #
    unix_timestamp: Annotated[DatetimeUnixTimestamp, TypeAffinityRepr(float)]
    unix_timestamp_int: Annotated[DatetimeUnixTimestampInt, TypeAffinityRepr(int)]
    datetime_iso8601: Annotated[DatetimeISO8601, TypeAffinityRepr(str)]

    # ------ enums ------ #
    choice_abc: Annotated[
        ChoiceABC,
        TypeAffinityRepr(ChoiceABC),
        ConstrainRepr(
            "NOT NULL",
            ("CHECK", r'(choice_abc IN ("A", "B", "C"))'),
            ("DEFAULT", ChoiceABC.A),
        ),
    ] = ChoiceABC.A
    optional_choice_123: Annotated[
        Optional[Choice123],
        TypeAffinityRepr(Choice123),
        ConstrainRepr(
            (
                "CHECK",
                r"(optional_choice_123 is NULL OR optional_choice_123 IN (1, 2, 3))",
            ),
            ("DEFAULT", "NULL"),
        ),
    ] = None

    # ------ literals ------ #
    optional_num_literal: Annotated[
        Optional[SomeIntLiteral],
        TypeAffinityRepr(SomeIntLiteral),
        ConstrainRepr(
            (
                "CHECK",
                r"(optional_num_literal is NULL OR optional_num_literal IN (123, 456, 789))",
            ),
            ("DEFAULT", "NULL"),
        ),
    ] = None
    str_literal: Annotated[
        SomeStrLiteral,
        TypeAffinityRepr(SomeStrLiteral),
        ConstrainRepr(
            "NOT NULL",
            ("CHECK", r'(str_literal IN ("H", "I", "J"))'),
            ("DEFAULT", "H"),
        ),
    ] = "H"

    # ------ built-in types ------ #
    key_id: Annotated[int, TypeAffinityRepr(int), ConstrainRepr("NOT NULL")]
    # Here for convenience, all prim_ prefixed field can be
    #   derived by prim_key, check MyStr's methods for mor details.
    prim_key: Annotated[
        Mystr,
        TypeAffinityRepr(Mystr),
        ConstrainRepr("PRIMARY KEY", ("CHECK", r"(length(prim_key) <= 128)")),
    ]
    prim_key_sha256hash: Annotated[
        bytes, TypeAffinityRepr(bytes), ConstrainRepr("NOT NULL", "UNIQUE")
    ]
    prim_key_magicf: Annotated[
        float, TypeAffinityRepr(float), ConstrainRepr("NOT NULL")
    ]
    prim_key_bln: Annotated[bool, TypeAffinityRepr(bool), ConstrainRepr("NOT NULL")]
