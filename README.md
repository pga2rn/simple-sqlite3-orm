# Simple Python SQLite3 ORM

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=pga2rn_simple-sqlite3-orm&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=pga2rn_simple-sqlite3-orm)
[![codecov](https://codecov.io/gh/pga2rn/simple-sqlite3-orm/graph/badge.svg?token=UAE1NENEG7)](https://codecov.io/gh/pga2rn/simple-sqlite3-orm)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=pga2rn_simple-sqlite3-orm&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=pga2rn_simple-sqlite3-orm)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=pga2rn_simple-sqlite3-orm&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=pga2rn_simple-sqlite3-orm)
[![PyPI version](https://badge.fury.io/py/simple-sqlite3-orm.svg)](https://badge.fury.io/py/simple-sqlite3-orm)

A simple yet powerful SQLite3 ORM based on Python's sqlite3 DB engine, powered by pydantic.

It targets basic CRUD operations and does it well, while also opened to complicated use cases.

## Features and hightlights

- Light-weight sqlite3 ORM based on Python3's std sqlite3 DB engine, with only `pydantic` and `typing_extensions` as dependencies.
- Support defining your database and table as code.
- Provides simple and clean APIs for basic CRUD operatations.
- All functions and types are fully typed and docstrings applied.
- Opened to advanced and more complicated use cases with helper functions, extra APIs and sqlite3 specific constants.

## Natively supported Python types

Besides the Python types that sqlite3 directly supported,`simple-sqlite3-orm` also adds direct support to the following python types:

- Enums types: `IntEnum` and `StrEnum`.
- Literal types: str Literal and int Literal.
- Supported types that wrapped within Optional(like `Optional[str]`).

`simple-sqlite3-orm` also provides the following types for datetime support:

- `DatetimeUnixTimestamp`: will be serialized and stored as REAL in database.
- `DatetimeUnixTimestampInt`: will be serialized and stored as INTEGER in database.
- `DatetimeISO8601`: will be serialized into ISO8601 format string and stored as TEXT in database.

## Installation

```shell
pip install simple-sqlite3-orm
```

`simple-sqlite3-orm` supports Python 3.8+.

## Basic usage

`simple-sqlite3-orm` applies docstrings to most of the APIs, you can always refer to docstrings for help and more information.
Also, this chapter only shows usage of baisc CRUD operations, there are also many extra APIs available for advanced use cases.

For a more complicated example, see[sample_db](tests/sample_db).

### Define your table as code

`simple-sqlite3-orm` provides `TableSpec` as base for you to define table.

`TableSpec` subclasses pydantic's `BaseModel`, so you can follow your experience of using pydantic to define your table with ease.
With pydantic's powerful validation/serialization feature, you can also simply define custom type that mapping to sqlite3's data type following pydantic way.

```python
from typing import TypedDict, Literal
from simple_sqlite3_orm import ConstrainRepr, TableSpec, TypeAffinityRepr

# Optionally, you can define a TypedDict for select and delete related APIs' type hints.
#   Due to the limitation of Python typing system, currently there is no way to
#   use the defined TableSpec(pydantic model) to type hint the kwargs.
# See the following sections of select and delete db operations for more details.
class MyTableHint(TypedDict, total=False):
    # no need to copy and paste the full type annotations from the actual TableSpec, only the actual type is needed
    entry_id: int
    entry_type: Literal["A", "B", "C"]
    entry_token: bytes
    special_attrs: SpecialAttrsType

class MyTable(TableSpec):
    entry_id: Annotated[int, ConstrainRepr("PRIMARY KEY")]
    entry_type: Annotated[
        Literal["A", "B", "C"],
        ConstrainRepr("NOT NULL", ("CHECK", "entry_type IN (A,B,C)"))
    ]
    entry_token: bytes

    # A custom type that defines validator/serializer in pydantic way,
    #   this custom type is serialized into bytes and stored as BLOB in database.
    special_attrs: Annotated[SpecialAttrsType, TypeAffinityRepr(bytes), ConstrainRepr("NOT NULL")]
```

### Define your database as code

After the table definition is ready, you can further define ORM types.

`simple-sqlite3-orm` provides `ORMBase` for you to define the ORM with table you defined previously.
`ORMBase` supports defining database as code(table_name, table create configuration, indexes) for deterministically bootstrapping new empty database file.
You can do it as follow:

```python
from simple_sqlite3_orm import CreateIndexParams, CreateTableParams, ORMBase

class MyORM(ORMBase[MyTable]):

    orm_bootstrap_table_name = "my_table"
    orm_bootstrap_create_table_params = CreateTableParams(without_rowid=True)
    orm_bootstrap_indexes_params = [
        CreateIndexParams(index_name="entry_token_index", index_cols=("entry_token",))
    ]
```

### Bootstrap new database

After defining the ORM, you can bootstrap a new empty database, create table(and indexes) deterministically as follow:

```python3
import sqlite3

conn = sqlite3.connect("my_db.sqlite3")
orm = MyORM(conn)

orm.orm_bootstrap_db()
```

Alternatively, you can also use `orm_create_table` and `orm_create_index` separately to bootstrap a new database.

### Insert entries into database

You can use `orm_insert_entry` to insert exactly one entry:

```python
entry_to_insert: MyTable
orm.orm_insert_entry(entry_to_insert)
```

Or you can insert an Iterable that yields entries:

```python
entries_to_insert: Iterable[MyTable]
inserted_entries_count = orm.orm_insert_entries(entries_to_insert)
```

### Select entries from database

You can select entries by matching column(s) from database:

```python
res_gen: Generator[MyTable] = orm.orm_select_entries(entry_type="A", entry_token=b"abcdef")

# or using the defined TypedDict for type hints:
res_gen: Generator[MyTable] = orm.orm_select_entries(**MyTableHint(entry_type="A", entry_token=b"abcdef"))

for entry in res_gen:
    ...
```

### Delete entries from database

Like select operation, you can detele entries by matching column(s):

```python
affected_row_counts: int = orm.orm_delete_entries(entry_type="C")

# or using the defined TypedDict for type hints:
affected_row_counts: int = orm.orm_delete_entries(**MyTableHint(entry_type="C"))
```

## ORM pool support

`simple-sqlite3-orm` also provides ORM threadpool(`ORMThreadPoolBase`) and asyncio ORM(`AsyncORMBase`, experimental) supports.

ORM threadpool and asyncio ORM implements most of the APIs available in `ORMBase`, except for the `orm_conn` API.

## License

`simple-sqlite3-orm` is licensed under Apache 2.0 License.
