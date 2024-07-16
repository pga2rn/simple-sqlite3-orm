"""Custom types used in test sample_db."""

from __future__ import annotations

from enum import Enum
from functools import cached_property
from hashlib import sha256
from typing import Any, Literal

from pydantic import BeforeValidator, Field, GetCoreSchemaHandler
from pydantic_core import core_schema
from typing_extensions import Annotated


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

    # some defined custom methods

    @cached_property
    def sha256hash(self) -> bytes:
        return sha256(self.encode()).digest()

    @cached_property
    def magicf(self) -> float:
        return self.sha256hash[-1]

    @cached_property
    def bool(self) -> bool:
        return bool(self.sha256hash[-1] & 0x01)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """Let pydantci validate Mystr as normal str."""
        # NOTE: let pydantic treat this wrapper as str
        return core_schema.no_info_after_validator_function(cls, handler(str))


SomeIntLiteral = Literal[123, 456, 789]
SomeStrLiteral = Literal["H", "I", "J"]

NetworkPort = Annotated[
    int,
    BeforeValidator(lambda x: int(x)),
    Field(ge=1, le=65535),
]
"""A NetworkPort type that accepts port in number or string."""
