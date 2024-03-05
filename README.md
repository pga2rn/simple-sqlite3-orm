# Simple python SQLite3 ORM

## Usage

```python
class SomeTable(ORMBase):
    prim_key_mystr: Annotated[
        MyStr,
        TypeAffinityRepr(MyStr),
        ConstrainRepr("PRIMARY KEY"),
    ]
    optional_str_enum: Annotated[
        Optional[SomeStrEnum],
        TypeAffinityRepr(SomeStrEnum),
    ] = None
    some_i_enum: Annotated[
        SomeIntEnum,
        TypeAffinityRepr(SomeIntEnum),
        ConstrainRepr("NOT NULL", ("CHECK", "(some_i_enum > 0 AND some_i_enum < 4)")),
    ]
    bytes: Annotated[
        bytes,
        TypeAffinityRepr(bytes),
        ConstrainRepr("NOT NULL"),
    ]
    unique_myfloat: Annotated[
        MyFloat,
        TypeAffinityRepr(MyFloat),
        ConstrainRepr("UNIQUE"),
    ]
    optional_str_literal: Annotated[
        Optional[SomeStrLiteral],
        TypeAffinityRepr(SomeStrLiteral),
    ] = None
    default_i_literal: Annotated[
        SomeIntLiteral,
        TypeAffinityRepr(SomeIntLiteral),
        ConstrainRepr(
            "NOT NULL",
            ("CHECK", "(default_i_literal > 0 AND default_i_literal < 4)"),
            ("DEFAULT", "2"),
        ),
    ] = 2
```
