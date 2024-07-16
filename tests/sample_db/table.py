"""A simple table example for testing."""

from __future__ import annotations

from typing import Optional

from pydantic import SkipValidation
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
    concatenate_condition,
    gen_check_constrain,
    wrap_value,
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
            ("DEFAULT", wrap_value(ChoiceABC.A)),
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
            ("DEFAULT", wrap_value(None)),
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
            ),
            ("DEFAULT", wrap_value(None)),
        ),
        SkipValidation,
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
            ("DEFAULT", wrap_value("H")),
        ),
        SkipValidation,
    ] = "H"

    # ------ built-in types ------ #
    key_id: Annotated[
        int, TypeAffinityRepr(int), ConstrainRepr("NOT NULL"), SkipValidation
    ]
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
        SkipValidation,
    ]
    prim_key_sha256hash: Annotated[
        bytes,
        TypeAffinityRepr(bytes),
        ConstrainRepr("NOT NULL", "UNIQUE"),
        SkipValidation,
    ]
    prim_key_magicf: Annotated[
        float, TypeAffinityRepr(float), ConstrainRepr("NOT NULL"), SkipValidation
    ]
    prim_key_bln: Annotated[
        bool, TypeAffinityRepr(bool), ConstrainRepr("NOT NULL"), SkipValidation
    ]
