"""Database operation with sample table."""

from __future__ import annotations

from simple_sqlite3_orm import AsyncORMBase, ORMBase, ORMThreadPoolBase
from tests.conftest import TABLE_NAME
from tests.sample_db.table import SampleTable


class SampleDB(ORMBase[SampleTable]):
    """ORM for SampleTable."""

    orm_bootstrap_table_name = TABLE_NAME


class SampleDBAsyncio(AsyncORMBase[SampleTable]):
    """Test connection pool with async API."""

    orm_bootstrap_table_name = TABLE_NAME


class SampleDBConnectionPool(ORMThreadPoolBase[SampleTable]):
    """Test connection pool."""

    orm_bootstrap_table_name = TABLE_NAME
