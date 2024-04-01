"""A simple table example for testing."""

from __future__ import annotations

from enum import Enum
from functools import cached_property
from hashlib import sha256
from typing import Any, Literal, Optional

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema
from typing_extensions import Annotated

from simple_sqlite3_orm import (
    ConstrainRepr,
    TableSpec,
    TypeAffinityRepr,
    DatetimeISO8601,
    DatetimeUnix,
    DatetimeUnixNS,
)


class ChoiceABC(str, Enum):
    """A choice includes 'A', 'B' and 'C'."""

    A = "A"
    B = "B"
    C = "C"


class Choice123(int, Enum):
    """A choice includes 1, 2 and 3."""

    ONE = 1
    TWO = 2
    THREE = 3


class Mystr(str):
    """Custom str type that wraps built-in str."""

    @cached_property
    def hash(self) -> bytes:
        return sha256(self.encode()).digest()

    @cached_property
    def magicf(self) -> float:
        return self.hash[-1]

    @cached_property
    def bool(self) -> bool:
        return bool(self.hash[-1] & 0x01)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """Allow pydantic to support validating our str."""
        # NOTE: let pydantic treat this wrapper as str
        return core_schema.no_info_after_validator_function(cls, handler(str))


SomeIntLiteral = Literal[123, 456, 789]
SomeStrLiteral = Literal["H", "I", "J"]


class SampleTable(TableSpec):
    """This sample table contains as much different types of fields as possible."""

    # ------ datetime related ------ #
    datetime_unix_sec: Annotated[DatetimeUnix, TypeAffinityRepr(int)]
    datetime_iso8601: Annotated[DatetimeISO8601, TypeAffinityRepr(str)]
    datetime_unix_float: Annotated[DatetimeUnixNS, TypeAffinityRepr(float)]

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
            )
        ),
    ] = None

    # ------ literals ------ #
    optional_num_literal: Annotated[
        Optional[SomeIntLiteral],
        TypeAffinityRepr(SomeIntLiteral),
        ConstrainRepr(
            (
                "CHECK",
                r"(optional_num_literal is NULL OR optional_num_literal IN (7, 8, 9))",
            )
        ),
    ] = None
    str_literal: Annotated[
        SomeStrLiteral,
        TypeAffinityRepr(SomeStrLiteral),
        ConstrainRepr(
            "NOT NULL",
            ("CHECK", r'(optional_num_literal IN ("H", "I", "J"))'),
            ("DEFAULT", "H"),
        ),
    ] = "H"

    # ------ built-in types ------ #
    key_id: Annotated[int, TypeAffinityRepr(int), ConstrainRepr("PRIMARY KEY")]
    # Here for convenience, all prim_ prefixed field can be
    #   derived by prim_key, check MyStr's methods for mor details.
    prim_key: Annotated[
        Mystr,
        TypeAffinityRepr(Mystr),
        ConstrainRepr("PRIMARY KEY", ("CHECK", r"(length(prim_key) <= 32)")),
    ]
    prim_key_hash: Annotated[
        bytes, TypeAffinityRepr(bytes), ConstrainRepr("NOT NULL", "UNIQUE")
    ]
    prim_key_magicf: Annotated[
        float, TypeAffinityRepr(float), ConstrainRepr("NOT NULL")
    ]
    prim_key_bln: Annotated[bool, TypeAffinityRepr(bool), ConstrainRepr("NOT NULL")]
