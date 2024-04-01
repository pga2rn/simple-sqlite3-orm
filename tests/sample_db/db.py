"""Database operation with sample table."""

from __future__ import annotations

from simple_sqlite3_orm import ORMBase
from tests.sample_db.table import SampleTable


class SampleDB(ORMBase[SampleTable]):
    """ORM for SampleTable."""
