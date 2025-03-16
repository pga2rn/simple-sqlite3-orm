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

Besides the sqlite3 natively supported python types,`simple-sqlite3-orm` also adds direct support to the following python types:

- Enums types: `IntEnum` and `StrEnum`.
- Literal types: str Literal and int Literal.
- Supported types that wrapped within Optional(like `Optional[str]`).

`simple-sqlite3-orm` also datetime support with the following types:

- `DatetimeUnixTimestamp`: will be serialized and stored as REAL in database.
- `DatetimeUnixTimestampInt`: will be serialized and stored as INTEGER in database.
- `DatetimeISO8601`: will be serialized into ISO8601 format string and stored as TEXT in database.

## Installation

```shell
pip install simple-sqlite3-orm
```

`simple-sqlite3-orm` supports Python 3.8+.

## Basic usage

This chapter only shows very basic(thus simple) usage of CRUD operations, there are also many extra APIs available for advanced use cases.

`simple-sqlite3-orm` applies docstrings to most of the APIs, you can always refer to docstrings for help and more information.

### Define your table as code

`simple-sqlite3-orm` provides `TableSpec` as base for you to define table.

`TableSpec` subclasses pydantic's `BaseModel`, so you can follow your experience of using pydantic to define your table as code.

Also, it is recommended to define a `TypedDict` for your table. All CRUD ORM APIs support taking mappings as params, you can utilize the TypedDict for using these APIS with type hint.

```python
from typing import TypedDict, Literal
from simple_sqlite3_orm import ConstrainRepr, TableSpec, TypeAffinityRepr

# ------ Table definition ------ #

class MyTable(TableSpec):
    entry_id: Annotated[int, ConstrainRepr("PRIMARY KEY")]
    entry_type: Annotated[
        Literal["A", "B", "C"],
        ConstrainRepr("NOT NULL", ("CHECK", "entry_type IN (A,B,C)"))
    ]
    entry_token: bytes

    # A custom type that defines serializer/deserializer in pydantic way,
    #   this custom type is serialized into bytes and stored as BLOB in database.
    special_attrs: Annotated[SpecialAttrsType, TypeAffinityRepr(bytes), ConstrainRepr("NOT NULL")]

# ------ Helper TypedDict for MyTable ------ #

class MyTableCols(TypedDict, total=False):
    # no need to copy and paste the full type annotations from the actual TableSpec,
    #   only the actual type is needed.
    entry_id: int
    entry_type: Literal["A", "B", "C"]
    entry_token: bytes
    special_attrs: SpecialAttrsType
```

For a more complicated example, see[sample_db](tests/sample_db).

### Define your database as code

After the table definition is ready, you can further define ORM types.

`simple-sqlite3-orm` provides `ORMBase` for you to define the ORM with table you defined previously.
`ORMBase` supports defining database as code with specifying table_name, table create configuration and indexes for deterministically bootstrapping new empty database file.

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

```python
import sqlite3

conn = sqlite3.connect("my_db.sqlite3")
orm = MyORM(conn)

orm.orm_bootstrap_db()
```

Alternatively, you can also use `orm_create_table` and `orm_create_index` separately to bootstrap a new database.

### Insert rows

You can use `orm_insert_entry` or `orm_insert_mapping` to insert exactly one entry:

```python
entry_to_insert: MyTable
mapping_to_insert: MyTableCols

# insert a row by MyTable instance
orm.orm_insert_entry(entry_to_insert)

# insert a row by mapping as MyTableCols TypedDict
#   with a mapping, you can insert partially set row and let DB engine fill
#   the unprovided cols with DEFAULT value or NULL.
orm.orm_insert_mapping(mapping_to_insert)
```

Or you can insert a a bunch of entries by an Iterable that yields entries:

```python
entries_to_insert: Iterable[MyTable]
mappings_to_insert: Iterable[MyTableCols]

inserted_entries_count = orm.orm_insert_entries(entries_to_insert)
inserted_entries_count = orm.orm_insert_mappings(mappings_to_insert)
```

### Select rows

You can select entries by matching column(s) from database:

```python
res_gen: Generator[MyTable] = orm.orm_select_entries(MyTableCols(entry_type="A", entry_token=b"abcdef"))

for entry in res_gen:
    # do something to each fetched entry here
    ...
```

### Update rows

You can update specific rows with one set of params as follow:

```python
# specify rows by matching cols
#   WHERE stmt will be generated from `where_cols_value`.
orm.orm_update_entries(
    set_values=MyTableCols(entry_token="ccddee123", entry_type="C"),
    where_cols_value=MyTableCols(entry_id=123),
)

# alteratively, you can directly provide the WHERE stmt and `extra_params` for the query execution.
#   be careful to not use the columns's named-placeholder used by `set_values`.
orm.orm_update_entries(
    set_values=MyTableCols(entry_token="ccddee123", entry_type="C"),
    where_stmt="WHERE entry_id > :entry_lower_bound AND entry_id < :entry_upper_bound",
    _extra_params={"entry_lower_bound": 123, "entry_upper_bound": 456}
)
```

Also, there is an `executemany` version of ORM update API, `orm_update_entries_many`, which you can use many sets of params for the same UPDATE query execution.

Using this API is **SIGNIFICANTLY** faster with lower memory usage than calling `orm_update_entries` each time in a for loop.

```python
set_cols_value_iter: Iterable[MyTableCols]
where_cols_value_iter: Iterable[Mapping[str, Any]]

updated_rows_count: int = orm.orm_update_entries_many(
    set_cols=("entry_id", "entry_token", "entry_type"),
    where_cols=("entry_id",),
    set_cols_value=set_cols_value_iter,
    where_cols_value=where_cols_value_iter,
)
```

### Delete rows

Like select operation, you can detele entries by matching column(s):

```python
affected_row_counts: int = orm.orm_delete_entries(entry_type="C")

# or using the defined TypedDict:
affected_row_counts: int = orm.orm_delete_entries(MyTableCols(entry_type="C"))
```

## ORM pool support

`simple-sqlite3-orm` also provides ORM threadpool(`ORMThreadPoolBase`) and asyncio ORM(`AsyncORMBase`, experimental) supports.

ORM threadpool and asyncio ORM implements most of the APIs available in `ORMBase`, except for the `orm_conn` API.

## License

`simple-sqlite3-orm` is licensed under Apache 2.0 License.
