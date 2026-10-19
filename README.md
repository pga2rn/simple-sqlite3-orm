# Simple Python SQLite3 ORM

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=pga2rn_simple-sqlite3-orm&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=pga2rn_simple-sqlite3-orm)
[![codecov](https://codecov.io/gh/pga2rn/simple-sqlite3-orm/graph/badge.svg?token=UAE1NENEG7)](https://codecov.io/gh/pga2rn/simple-sqlite3-orm)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=pga2rn_simple-sqlite3-orm&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=pga2rn_simple-sqlite3-orm)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=pga2rn_simple-sqlite3-orm&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=pga2rn_simple-sqlite3-orm)
[![Supported Python version](https://img.shields.io/pypi/pyversions/simple-sqlite3-orm.svg)](https://pypi.org/project/simple-sqlite3-orm/)
[![PyPI version](https://badge.fury.io/py/simple-sqlite3-orm.svg)](https://pypi.org/project/simple-sqlite3-orm/)

A simple yet powerful SQLite3 ORM built on Python's standard library `sqlite3` module, powered by pydantic.

It targets basic CRUD operations and does them well, while staying open to more complicated use cases.

## Features and highlights

- Light-weight ORM on top of Python's standard `sqlite3` module, with only `pydantic` and `typing_extensions` as dependencies.
- Define your tables and databases as code, and bootstrap new database files deterministically.
- Simple and clean APIs for basic CRUD operations, with `executemany` variants for bulk writes.
- All functions and types are fully typed, with docstrings.
- Thread pool (`ORMThreadPoolBase`) and asyncio (`AsyncORMBase`) variants that expose the same API.
- Open to advanced use cases with raw-SQL escape hatches, helper functions, extra APIs and sqlite3-specific constants.

## Installation

```shell
pip install simple-sqlite3-orm
```

Or add the `simple-sqlite3-orm` as your project dependency with [uv](https://docs.astral.sh/uv/):

```shell
uv add simple-sqlite3-orm
```

`simple-sqlite3-orm` supports Python 3.8+, including Python 3.14 and the free-threaded 3.14t build.

## Natively supported Python types

Besides the Python types that `sqlite3` supports natively (`int`, `float`, `str`, `bytes` and `None`), `simple-sqlite3-orm` maps the following annotations to SQLite column types automatically:

- Enum types: `IntEnum` and `StrEnum`.
- Literal types: `Literal` of `str` values or of `int` values.
- Any of the above wrapped in `Optional` (like `Optional[str]`).

`simple-sqlite3-orm` also ships `datetime.datetime` helper types that define how the value is serialized for the database:

- `DatetimeUnixTimestamp`: serialized as a unix timestamp `float`.
- `DatetimeUnixTimestampInt`: serialized as a unix timestamp `int`.
- `DatetimeISO8601`: serialized as an ISO 8601 formatted `str`.

These helper types only define the serializer and validator. Pair them with `TypeAffinityRepr` to set the column type, e.g. `Annotated[DatetimeUnixTimestamp, TypeAffinityRepr(float)]` for a `REAL` column.

Any other type can be stored by defining its database form with pydantic validators and serializers, then setting the column type with `TypeAffinityRepr`. See [sample_db](tests/sample_db) for examples.

## Basic usage

This chapter only shows very basic usage of CRUD operations. Many extra APIs are available for advanced use cases; most APIs carry docstrings, so refer to them for details.

### Define your table as code

`simple-sqlite3-orm` provides `TableSpec` as the base class for table definitions.

`TableSpec` subclasses pydantic's `BaseModel`, so defining a table works just like defining a pydantic model. Column types are inferred from the annotations, and `Annotated` metadata adds SQL details:

- `TypeAffinityRepr` sets the column type explicitly, for types that cannot be inferred.
- `ConstrainRepr` adds column constraints such as `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `CHECK` and `DEFAULT`.

For a more complicated example, see [sample_db](tests/sample_db).

```python
from typing import Literal

from typing_extensions import Annotated

from simple_sqlite3_orm import ConstrainRepr, TableSpec, TypeAffinityRepr


class MyTable(TableSpec):
    entry_id: Annotated[int, ConstrainRepr("PRIMARY KEY")]
    entry_type: Annotated[
        Literal["A", "B", "C"],
        ConstrainRepr("NOT NULL", ("CHECK", "(entry_type IN ('A', 'B', 'C'))")),
    ]
    entry_token: bytes
    # A column whose type cannot be inferred from the annotation:
    #   a custom type with pydantic validator/serializer, stored as BLOB.
    special_attrs: Annotated[SpecialAttrsType, TypeAffinityRepr(bytes), ConstrainRepr("NOT NULL")]
```

`simple_sqlite3_orm.utils` provides helpers like `gen_check_constrain`, `concatenate_condition` and `wrap_value` for composing `CHECK` and `DEFAULT` expressions from enums and literals.

### (Recommended) Define typing helpers for your table

It is recommended to define two typing helpers for your table:

1. A `TypedDict` for building column/value mappings with type checking.
2. A `ColsSelectFactory` for building tuples of column names with type checking and completion suggestions.

`simple-sqlite3-orm` APIs take mappings as parameters, and tuples for column selections.
With these helpers, your type checker catches column name typos and column renames that were not propagated to every call site.

```python
from typing import Literal, TypedDict

from simple_sqlite3_orm import ColsSelectFactory

# ------------ TypedDict ------------ #

# NOTE: `total=False` allows specifying only some of the col/value pairs.
class MyTableCols(TypedDict, total=False):
    # No need to copy the full annotations from the TableSpec,
    #   only the actual Python types are needed.
    entry_id: int
    entry_type: Literal["A", "B", "C"]
    entry_token: bytes
    special_attrs: SpecialAttrsType

# examples:
good_row = MyTableCols(entry_id=123)
bad_row_type_invalid = MyTableCols(entry_id="not_an_int")  # type check error
bad_row_unknown_col = MyTableCols(unknown="unknown")  # type check error

# ------------ ColsSelector ------------ #

MyTableColsSelector = ColsSelectFactory[Literal["entry_id", "entry_type", "entry_token", "special_attrs"]]

# examples:
good_cols_selection = MyTableColsSelector("entry_id", "special_attrs")
bad_cols_selection = MyTableColsSelector("entyr_di")  # type check error
```

### Define your database as code

With the table definition ready, define the ORM type for it.

`simple-sqlite3-orm` provides `ORMBase` for defining the ORM over a table. Subscript it with your `TableSpec` subclass.
`ORMBase` supports defining the database as code: specify the table name, the `CREATE TABLE` options and the indexes, so a new empty database file can be bootstrapped deterministically.

```python
from simple_sqlite3_orm import CreateIndexParams, CreateTableParams, ORMBase


class MyORM(ORMBase[MyTable]):
    orm_bootstrap_table_name = "my_table"
    orm_bootstrap_create_table_params = CreateTableParams(without_rowid=True)
    orm_bootstrap_indexes_params = [
        CreateIndexParams(index_name="entry_token_index", index_cols=MyTableColsSelector("entry_token"))
    ]
```

### Bootstrap a new database

After defining the ORM, bootstrap a new empty database and create the table (and indexes) deterministically:

```python
import sqlite3

conn = sqlite3.connect("my_db.sqlite3")
orm = MyORM(conn)

orm.orm_bootstrap_db()
```

Alternatively, call `orm_create_table` and `orm_create_index` separately.

An `ORMBase` instance can also be used as a context manager, which closes the connection on exit.
The `table_name` and `schema_name` init parameters override the class-level table name, so one ORM type can serve several tables with the same schema.

Note that an `ORMBase` instance wraps a single `sqlite3.Connection` and must not be shared across threads; see [ORM pool support](#orm-pool-support) for that.

### Insert rows

Use `orm_insert_entry` or `orm_insert_mapping` to insert exactly one row:

```python
entry_to_insert: MyTable
mapping_to_insert: MyTableCols

# insert a row from a MyTable instance
orm.orm_insert_entry(entry_to_insert)

# insert a row from a mapping (here the MyTableCols TypedDict).
#   With a mapping you can insert a partially specified row and let the
#   DB engine fill the missing columns with their DEFAULT value or NULL.
orm.orm_insert_mapping(mapping_to_insert)
```

Or insert many rows at once from an iterable, which runs a single `executemany`:

```python
entries_to_insert: Iterable[MyTable]
mappings_to_insert: Iterable[MyTableCols]

inserted_entries_count = orm.orm_insert_entries(entries_to_insert)
inserted_entries_count = orm.orm_insert_mappings(mappings_to_insert)
```

All insert APIs accept an `or_option` keyword (`"replace"`, `"ignore"`, ...) to generate `INSERT OR ...` statements.

### Select rows

Select rows by matching column value(s). The result is a generator that yields `MyTable` instances by default:

```python
res_gen = orm.orm_select_entries(MyTableCols(entry_type="A", entry_token=b"abcdef"))

# process each selected entry here
for entry in res_gen: ...

# keyword arguments work as well, and `_order_by`, `_limit` and `_distinct` are available:
for entry in orm.orm_select_entries(entry_type="A", _order_by=("entry_id",), _limit=10): ...
```

Related APIs:

- `orm_select_entry` returns the first matching row, or `None`.
- `orm_check_entry_exist` returns whether at least one row matches.
- `orm_select_all_with_pagination(batch_size=...)` iterates over the whole table in batches. It seeks by `rowid`, so it does not work on `WITHOUT ROWID` tables like the `MyTable` example above.

### Update rows

Update specific rows with one set of values:

```python
# specify the rows by matching columns:
#   the WHERE statement will be generated from `where_cols_value`.
orm.orm_update_entries(
    set_values=MyTableCols(entry_token=b"ccddee123", entry_type="C"),
    where_cols_value=MyTableCols(entry_id=123),
)

# alternatively, provide the WHERE statement directly, with `_extra_params`
#   for the named placeholders it uses.
#   Be careful not to reuse the named placeholders of the columns in `set_values`.
orm.orm_update_entries(
    set_values=MyTableCols(entry_token=b"ccddee123", entry_type="C"),
    where_stmt="WHERE entry_id > :entry_lower_bound AND entry_id < :entry_upper_bound",
    _extra_params={"entry_lower_bound": 123, "entry_upper_bound": 456},
)
```

There is also an `executemany` version, `orm_update_entries_many`, which runs the same UPDATE statement with many sets of parameters.
It is **significantly** faster and uses less memory than calling `orm_update_entries` in a loop.

```python
set_cols_value_iter: Iterable[MyTableCols]
where_cols_value_iter: Iterable[MyTableCols]

updated_rows_count: int = orm.orm_update_entries_many(
    set_cols=MyTableColsSelector("entry_token", "entry_type"),
    where_cols=MyTableColsSelector("entry_id"),
    set_cols_value=set_cols_value_iter,
    where_cols_value=where_cols_value_iter,
)
```

### Delete rows

Like select, delete rows by matching column value(s):

```python
affected_rows_count: int = orm.orm_delete_entries(MyTableCols(entry_type="C"))
```

`orm_delete_entries_with_returning` does the same but yields the deleted rows (via SQLite's `RETURNING` clause).

### Run raw SQL

When the ORM APIs are not enough, run SQL statements on the same connection while keeping the row factory:

```python
rows = orm.orm_execute(
    "SELECT * FROM my_table WHERE entry_id > :lower_bound;",
    {"lower_bound": 100},
)

# generator version for large results
for entry in orm.orm_execute_gen("SELECT * FROM my_table;"): ...
```

`orm_executemany` and `orm_executescript` are also available.
The `gen_sql_stmt` helper and the constants in `simple_sqlite3_orm` (`SQLiteTypeAffinity`, `SQLiteStorageClass`, `SQLiteBuiltInFuncs`, ...) help with composing statements.

### Row factory

By default an ORM sets the connection's `row_factory` so that rows come back as validated `TableSpec` instances.
The `row_factory` init parameter accepts:

- `"table_spec"` (default): rows are parsed and validated into `TableSpec` instances.
- `"table_spec_no_validation"`: rows are constructed without pydantic validation, which is faster.
- `"sqlite3_row_factory"`: rows are `sqlite3.Row` objects.
- `None`: rows are plain tuples.
- `"do_not_change"`: keep whatever `row_factory` the connection already has.
- any custom `sqlite3` row factory callable.

Select and execute APIs also accept a per-query `_row_factory` / `row_factory` keyword.

## ORM pool support

`simple-sqlite3-orm` also provides a thread pool ORM (`ORMThreadPoolBase`) and an asyncio ORM (`AsyncORMBase`, experimental).

Both implement the same APIs as `ORMBase`, except for `orm_con`, since each worker thread owns its own connection.
Instead of a connection, they take a `con_factory` callable and the number of connections (worker threads) to open.
WAL mode is recommended for concurrent access, see [SQLite's documentation](https://www.sqlite.org/wal.html#concurrency).

```python
import sqlite3
from functools import partial

from simple_sqlite3_orm import AsyncORMBase, ORMThreadPoolBase


class MyORMPool(ORMThreadPoolBase[MyTable]):
    orm_bootstrap_table_name = "my_table"


class MyAsyncORM(AsyncORMBase[MyTable]):
    orm_bootstrap_table_name = "my_table"


con_factory = partial(sqlite3.connect, "my_db.sqlite3", check_same_thread=False)

# thread pool: each API call is dispatched to a worker thread with its own connection
with MyORMPool(con_factory=con_factory, number_of_cons=4) as orm_pool:
    for entry in orm_pool.orm_select_entries(entry_type="A"): ...

# asyncio: the same APIs become coroutines. Generator APIs return an
#   async generator when awaited. Create the instance inside the running event loop.
async def main():
    async_orm = MyAsyncORM(con_factory=con_factory, number_of_cons=4)
    try:
        async for entry in await async_orm.orm_select_entries(entry_type="A"): ...
    finally:
        async_orm.orm_pool_shutdown()
```

## Utilities

`simple_sqlite3_orm.utils` provides connection and PRAGMA helpers, including `enable_wal_mode`, `enable_mmap`, `enable_tmp_store_at_memory`, `optimize_db`, `check_db_integrity`, `lookup_table`, `attach_database` and `check_pragma_compile_time_options`.

## License

`simple-sqlite3-orm` is licensed under the Apache 2.0 License.
