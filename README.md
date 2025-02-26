# Simple python SQLite3 ORM

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=pga2rn_simple-sqlite3-orm&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=pga2rn_simple-sqlite3-orm)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=pga2rn_simple-sqlite3-orm&metric=coverage)](https://sonarcloud.io/summary/new_code?id=pga2rn_simple-sqlite3-orm)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=pga2rn_simple-sqlite3-orm&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=pga2rn_simple-sqlite3-orm)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=pga2rn_simple-sqlite3-orm&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=pga2rn_simple-sqlite3-orm)
[![PyPI version](https://badge.fury.io/py/simple-sqlite3-orm.svg)](https://badge.fury.io/py/simple-sqlite3-orm)

A simple yet powerful SQLite3 ORM based on Python's sqlite3 DB engine, powered by pydantic.

It is very light-weight ORM, targets basic CRUD operations, does it well, and also opened to complicated use cases.

## Features and hightlights

1. Light-weight sqlite3 ORM based on Python3's std sqlite3 DB engine, with only `pydantic` and `typing_extensions` as dependencies.

2. Support defining database as code with ease.

3. Provides simple and clean APIs for basic CRUD operatations.

4. All functions and types are fully typed and docstrings applied.

5. Opened to advanced and more complicated use cases with helper functions, extra APIs and sqlite3 specific constants.

## Installation

`simple-sqlite3-orm` is published on pypi.

```shell
pip install simple-sqlite3-orm
```

`simple-sqlite3-orm` supports Python 3.8+.

## Basic usage

### Natively supported data types by `simple-sqlite3-orm`

Besides the Python native types that sqlite3 directly supported,`simple-sqlite3-orm` also add direct supports to the following python types:

1. Enums types: IntEnum and StrEnum.
2. Literal types: str Literal or int Literal.
3. native supported types that wrapped within Optional.

Also, `simple-sqlite3-orm` provides datetime support with the following types:

1. DatetimeUnixTimestamp: will be serialized and stored as REAL in database.
2. DatetimeUnixTimestampInt: will be serialized and stored as INTEGER in database.
3. DatetimeISO8601: will be serialized into ISO8601 format string and stored as TEXT in database.

### Define table as code with `TableSpec`

`simple-sqlite3-orm` provides `TableSpec` as base for you to define table.
`TableSpec` subclasses pydantic's `BaseModel`, so you can follow your experience of using pydantic to define your table with ease.

With pydantic's powerful validation/serialization feature, you can simply define custom type that mapping to sqlite3's data type following pydantic way.

Example custom type:
```python
from typing import NamedTuple

import msgpack
from pydantic import PlainSerializer, PlainValidator
from simple_sqlite3_orm import TypeAffin
from typing_extensions import Annotated

class SpecialAttrs(NamedTuple):
    """Custom type that stored as msgpack bytes in database."""

    attr_a: int
    attr_b: str
    
    @classmethod
    def _validator(cls, _in: bytes):
        # msgpack unpack the bytes back to python object
        ...

    def _serializer(self) -> bytes:
        # msgpack the python object to bytes
        ...

SpecialAttrsType = Annotated[
    SpecialAttrs,
    TypeAffinityRepr(bytes),
    PlainValidator(SpecialAttrs._validator),
    PlainSerializer(SpecialAttrs._serializer),
]
```

Define your table as follow:

```python
from simple_sqlite3_orm import (
    ConstrainRepr,
    TableSpec,
)

class MyTable(TableSpec):
    entry_id: Annotated[int, ConstrainRepr("PRIMARY KEY")]
    entry_type: Annotated[
        Literal["A", "B", "C"],
        ConstrainRepr("NOT NULL", ("CHECK", "entry_type IN (A,B,C)"))
    ]
    entry_token: bytes
    entry_contents: str | None = None
    special_attrs: Annotated[
        SpecialAttrsType,
        ConstrainRepr("NOT NULL")
    ] 
```

### Define database as code with `ORMBase`

After the table definition is ready, you can further define ORM types.

`simple-sqlite3-orm` provides `ORMBase` for you to define the ORM with table you defined previously. 
`ORMBase` supports defining database as code(table_name, table create configuration, indexes) for deterministically bootstrapping new empty database file. 
You can do it as follow:

```python3
from simple_sqlite3_orm import (
    CreateIndexParams,
    CreateTableParams,
    ORMBase,
)

class MyORM(ORMBase[MyTable]):

    orm_bootstrap_table_name = "my_table"
    orm_bootstrap_create_table_params = CreateTableParams(without_rowid=True)
    orm_bootstrap_indexes_params = [
        CreateIndexParams(index_name="entry_token_index", index_cols=("entry_token",))
    ]
```

### Bootstrap new database

After defining the ORM, you can bootstrap a new database deterministically as follow:

```python3
import sqlite3

conn = sqlite3.connect("my_db.sqlite3")
orm = MyORM(conn)

orm.orm_bootstrap_db()
```

Alternatively, you can also use `orm_create_table` and `orm_create_index` separately to bootstrap a new database.

### Insert entries into database

You can use `orm_insert_entry` to insert exactly one entry:

```python3
entry_to_insert: MyTable
orm: MyORM

orm.orm_insert_entry(entry_to_insert)
```

Or you can insert an Iterable that yields entries:

```python3
entries_to_insert: Iterable[MyTable]
orm: MyORM

inserted_entries_count = orm.orm_insert_entries(entries_to_insert)
```

### Select entries from database

You can select entries by matching column(s) from database:

```python3
orm: MyORM

res_gen: Generator[MyTable] = orm.orm_select_entries(entry_type="A", entry_token=b"abcdef")

# you can iter throught the result as follow
for entry in res_gen:
    ...
```

### Delete entries from database

Like select operation, you can detele entries by matching column(s):

```python3
orm: MyORM

affected_row_counts: int = orm.orm_delete_entries(entry_type="C")
```

# License

`simple-sqlite3-orm` is licensed under Apache 2.0 License.