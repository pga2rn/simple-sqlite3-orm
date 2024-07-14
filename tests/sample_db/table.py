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
from simple_sqlite3_orm.utils import (
    gen_check_constrain,
    concatenate_condition,
    default_constrain,
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
            (
                "CHECK",
                concatenate_condition(
                    gen_check_constrain(ChoiceABC, "choice_abc"),
                ),
            ),
            default_constrain(ChoiceABC.A),
        ),
    ] = ChoiceABC.A
    optional_choice_123: Annotated[
        Optional[Choice123],
        TypeAffinityRepr(Choice123),
        ConstrainRepr(
            (
                "CHECK",
                concatenate_condition(
                    "optional_choice_123",
                    "IS NULL",
                    "OR",
                    gen_check_constrain(Choice123, "optional_choice_123"),
                ),
            ),
            default_constrain(None),
        ),
    ] = None

    # ------ literals ------ #
    optional_num_literal: Annotated[
        Optional[SomeIntLiteral],
        TypeAffinityRepr(SomeIntLiteral),
        ConstrainRepr(
            (
                "CHECK",
                concatenate_condition(
                    "optional_num_literal",
                    "IS NULL",
                    "OR",
                    gen_check_constrain(SomeIntLiteral, "optional_num_literal"),
                ),
                default_constrain(None),
            ),
        ),
    ] = None
    str_literal: Annotated[
        SomeStrLiteral,
        TypeAffinityRepr(SomeStrLiteral),
        ConstrainRepr(
            "NOT NULL",
            (
                "CHECK",
                concatenate_condition(
                    gen_check_constrain(SomeStrLiteral, "str_literal"),
                ),
            ),
            default_constrain("H"),
        ),
    ] = "H"

    # ------ built-in types ------ #
    key_id: Annotated[int, TypeAffinityRepr(int), ConstrainRepr("NOT NULL")]
    # Here for convenience, all prim_ prefixed field can be
    #   derived by prim_key, check MyStr's methods for mor details.
    prim_key: Annotated[
        Mystr,
        TypeAffinityRepr(Mystr),
        ConstrainRepr(
            "PRIMARY KEY",
            (
                "CHECK",
                concatenate_condition("length(prim_key)", "=", "128"),
            ),
        ),
    ]
    prim_key_sha256hash: Annotated[
        bytes, TypeAffinityRepr(bytes), ConstrainRepr("NOT NULL", "UNIQUE")
    ]
    prim_key_magicf: Annotated[
        float, TypeAffinityRepr(float), ConstrainRepr("NOT NULL")
    ]
    prim_key_bln: Annotated[bool, TypeAffinityRepr(bool), ConstrainRepr("NOT NULL")]
