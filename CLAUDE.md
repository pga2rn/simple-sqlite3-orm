# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`simple-sqlite3-orm`: a small ORM over the stdlib `sqlite3` module, with `pydantic` v2 and `typing_extensions` as the only runtime dependencies. Supports Python 3.8 through 3.14 (including free-threaded 3.14t), so all source must stay 3.8-compatible (`from __future__ import annotations`, `typing_extensions` for newer typing features, ruff `target-version = "py38"`).

## Commands

Dependencies are managed with `uv` (lockfile `uv.lock`, dev deps in the `dev` dependency group).

```shell
uv sync                                   # install runtime + dev deps into .venv
uv run pytest                             # full test suite (testpaths = ./tests, pythonpath = src)
uv run pytest tests/test__orm.py          # one file
uv run pytest tests/test__orm.py::TestORMBase::test_select_entries   # one test
uv run pytest -k "update_entries"         # by keyword
uv run coverage run -m pytest && uv run coverage report   # what CI runs
uv run ruff check --fix . && uv run ruff format .         # lint + format
uv build                                  # build sdist/wheel (hatchling + hatch-vcs)
```

Pre-commit (`.pre-commit-config.yaml`) runs ruff, ruff-format, pyproject-fmt and markdownlint. The version comes from git tags via hatch-vcs and is written to `src/simple_sqlite3_orm/_version.py` (generated, excluded from coverage).

CI also runs the suite inside Ubuntu 18.04 to 24.04 containers (`tests/container/`) to cover old system SQLite versions. Tests that need newer SQLite features (`RETURNING`, `STRICT`, `SQLITE_ENABLE_UPDATE_DELETE_LIMIT`) gate themselves with `pytest.mark.skipif` on the `SQLITE3_FEATURE_FLAGS` class in `tests/conftest.py`. Keep doing that for any new feature-dependent test.

## Architecture

All public names are re-exported from `simple_sqlite3_orm/__init__.py`; the implementation lives in underscore-prefixed modules. `simple_sqlite3_orm.utils` (no underscore) is a public helper module for PRAGMA/connection tweaks and SQL-expression builders.

The library has two layers:

### 1. `TableSpec` (`_table_spec.py`): table definition and SQL statement generation

`TableSpec` is a pydantic `BaseModel` subclass. Column definitions are plain pydantic fields; SQL details are attached via `Annotated` metadata (`_utils.py`):

- `TypeAffinityRepr(<py type or affinity str>)` sets the column's SQLite type. If absent, the affinity is inferred from the annotation by `map_type` (int/str/bytes/float, `IntEnum`/`StrEnum`, `Literal`, and `Optional` of these). Anything else needs an explicit `TypeAffinityRepr` plus pydantic serializer/validator.
- `ConstrainRepr("NOT NULL", ("DEFAULT", "..."), ("CHECK", "..."))` renders column constraints.

`__pydantic_init_subclass__` populates `table_columns` / `table_columns_by_index`; column order in the class body is the schema column order. The `table_*_stmt` classmethods (`table_create_stmt`, `table_insert_stmt`, `table_select_stmt`, `table_update_stmt`, `table_delete_stmt`, `table_create_index_stmt`) generate parameterized SQL text with named placeholders (`:col`) and are `lru_cache`d, so their arguments must be hashable (tuples, not lists). `table_row_factory` / `table_from_tuple` / `table_from_dict` turn rows back into instances, and `table_dump_asdict` / `table_dump_astuple` / `table_serialize_mapping` serialize values for the DB via pydantic's dump.

The ORM layer never builds SQL itself; it calls these classmethods and passes the result to `sqlite3`.

### 2. ORM classes (`_orm/`): execute statements on a connection

- `ORMBase[TableSpec]` (`_orm/_base.py`) wraps one `sqlite3.Connection`. Subscripting with a concrete `TableSpec` subclass is not just typing: `parameterized_class_getitem` (`_orm/_utils.py`) creates and caches a new subclass with `orm_table_spec` set. Subclasses declare `orm_bootstrap_table_name`, `orm_bootstrap_create_table_params` and `orm_bootstrap_indexes_params` as class attributes; `ORMCommonBase.__init_subclass__` copies the table name into `_orm_table_name` (the old attribute name, still honored with a deprecation warning). The `table_name` / `schema_name` init args override the class-level name.
- `ORMThreadPoolBase` (`_orm/_pool.py`) owns a `ThreadPoolExecutor` whose initializer creates one `ORMBase` per worker thread from a `con_factory`. Every public method is `ORMBase`'s method wrapped by `_wrap_with_thread_ctx` (submit to pool, block on result) or `_wrap_generator_with_thread_ctx` (worker pushes rows into a bounded `queue.Queue`, caller yields from it; a sentinel and an exit `Event` handle termination, and an `atexit` hook drains queues on interpreter shutdown).
- `AsyncORMBase` subclasses the thread pool and swaps the wrappers for `_wrap_with_async_ctx` / `_wrap_generator_with_async_ctx`, which bind to the running loop at construction time (so instances must be created inside a coroutine). Generator APIs return an async generator from an awaited call.

Because the pool classes mirror `ORMBase` method-by-method, adding or renaming a public `orm_*` method on `ORMBase` requires adding the matching wrapped assignment in both `ORMThreadPoolBase` and `AsyncORMBase`, and tests in all three `tests/test_e2e/test_*_orm.py` files.

`row_factory` handling: the ORM sets the connection-scope `row_factory` at init (default `"table_spec"`, which validates rows into `TableSpec` instances), and select/execute APIs accept a per-call override. See `RowFactorySpecifier` in `_orm/_base.py` for the accepted values.

## Tests

- `tests/conftest.py` defines `SimpleTableForTest` (a minimal table used by unit tests) and connection fixtures that enable WAL, mmap and in-memory temp store, then assert `check_db_integrity` on teardown.
- `tests/sample_db/` is the "kitchen sink" table (`SampleTable`) exercising every supported column type plus custom pydantic-serialized types; the README points users to it as the reference example, so keep it representative.
- `tests/test_e2e/` runs CRUD against 120k generated rows for each of the sync, thread pool and asyncio ORMs. Test methods inside each class share a class-scoped connection and are written as an ordered sequence (create table, insert, index, select, delete), so running a single e2e method in isolation will fail on missing table/data. `asyncio_mode = "auto"` with a class-scoped loop is configured in `pyproject.toml`.
