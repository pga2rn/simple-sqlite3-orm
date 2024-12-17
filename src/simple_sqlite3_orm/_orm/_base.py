from __future__ import annotations

import sqlite3
from functools import cached_property, partial
from itertools import count
from typing import (
    Any,
    Generator,
    Generic,
    Iterable,
    Literal,
    TypeVar,
    Union,
)
from weakref import WeakValueDictionary

from typing_extensions import ParamSpec

from simple_sqlite3_orm._sqlite_spec import INSERT_OR, ORDER_DIRECTION
from simple_sqlite3_orm._table_spec import TableSpec, TableSpecType
from simple_sqlite3_orm._types import ConnectionFactoryType, RowFactoryType
from simple_sqlite3_orm._utils import GenericAlias

_parameterized_orm_cache: WeakValueDictionary[
    tuple[type[ORMBase], type[TableSpec]], type[ORMBase[Any]]
] = WeakValueDictionary()


P = ParamSpec("P")
RT = TypeVar("RT")


RowFactorySpecifier = Union[
    RowFactoryType,
    Literal[
        "sqlite3_row_factory",
        "table_spec",
        "table_spec_no_validation",
    ],
    None,
]
"""Specifiy which row_factory to use.

For each option:
    1. RowFactoryType: specify arbitrary row_factory.
    2. None: do not set connection scope row_factory.
    3. "sqlite3_row_factory": set to use sqlite3.Row as row_factory.
    4. "table_spec": use TableSpec.table_row_factory as row_factory.
    5. "table_spec_no_validation": use TableSpec.table_row_factory as row_factory, but with validation=False.
"""


def row_factory_setter(
    con: sqlite3.Connection,
    table_spec: type[TableSpec],
    row_factory_specifier: RowFactorySpecifier,
) -> None:  # pragma: no cover
    """Helper function to set connection scope row_factory by row_factory_specifier."""
    if callable(row_factory_specifier):
        con.row_factory = row_factory_specifier
    elif row_factory_specifier == "table_spec":
        con.row_factory = table_spec.table_row_factory
    elif row_factory_specifier == "table_spec_no_validation":
        con.row_factory = partial(table_spec.table_row_factory, validation=False)
    elif row_factory_specifier == "sqlite3_row_factory":
        con.row_factory = sqlite3.Row
    # do nothing means not changing connection scope row_factory


class ORMBase(Generic[TableSpecType]):
    """ORM layer for <TableSpecType>.

    NOTE that instance of ORMBase cannot be used in multi-threaded environment.
        Use ORMThreadPoolBase for multi-threaded environment.
        For asyncio, use AsyncORMThreadPoolBase.

    The underlying connection can be used in multiple connection for accessing different table in
        the connected database.

    Attributes:
        con (sqlite3.Connection | ConnectionFactoryType): The sqlite3 connection used by this ORM, or a factory
            function that returns a sqlite3.Connection object on calling.
        table_name (str): The name of the table in the database <con> connected to. This field will take prior over the
            table_name specified by _orm_table_name attr.
        schema_name (str): The schema of the table if multiple databases are attached to <con>.
        row_factory (RowFactorySpecifier): The connection scope row_factory to use. Default to "table_sepc".
    """

    orm_table_spec: type[TableSpecType]
    _orm_table_name: str
    """table_name for the ORM. This can be used for pinning table_name when creating ORM object."""

    def __init__(
        self,
        con: sqlite3.Connection | ConnectionFactoryType,
        table_name: str | None = None,
        schema_name: str | Literal["temp"] | None = None,
        *,
        row_factory: RowFactorySpecifier = "table_spec",
    ) -> None:
        if table_name:
            self._orm_table_name = table_name
        if getattr(self, "_orm_table_name", None) is None:
            raise ValueError(
                "table_name must be either set by <table_name> init param, or by defining <_orm_table_name> attr."
            )
        self._schema_name = schema_name

        if isinstance(con, sqlite3.Connection):
            self._con = con
        elif callable(con):
            self._con = con()
        row_factory_setter(self._con, self.orm_table_spec, row_factory)

    def __class_getitem__(cls, params: Any | type[Any] | type[TableSpecType]) -> Any:
        # just for convienience, passthrough anything that is not type[TableSpecType]
        #   to Generic's __class_getitem__ and return it.
        # Typically this is for subscript ORMBase with TypeVar or another Generic.
        if not (isinstance(params, type) and issubclass(params, TableSpec)):
            return super().__class_getitem__(params)  # type: ignore

        key = (cls, params)
        if _cached_type := _parameterized_orm_cache.get(key):
            return GenericAlias(_cached_type, params)

        new_parameterized_ormbase: type[ORMBase] = type(
            f"{cls.__name__}[{params.__name__}]", (cls,), {}
        )
        new_parameterized_ormbase.orm_table_spec = params  # type: ignore
        _parameterized_orm_cache[key] = new_parameterized_ormbase
        return GenericAlias(new_parameterized_ormbase, params)

    @property
    def orm_con(self) -> sqlite3.Connection:
        """A reference to the underlying sqlite3.Connection.

        This is for directly executing sql stmts.
        """
        return self._con

    @cached_property
    def orm_table_name(self) -> str:
        """The unique name of the table for use in sql statement.

        If multiple databases are attached to <con> and <schema_name> is availabe,
            return "<schema_name>.<table_name>", otherwise return <table_name>.
        """
        return (
            f"{self._schema_name}.{self._orm_table_name}"
            if self._schema_name
            else self._orm_table_name
        )

    def orm_execute(
        self, sql_stmt: str, params: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> list[Any]:
        """Execute one sql statement and get the all the result.

        The result will be fetched with fetchall API and returned as it.

        Args:
            sql_stmt (str): The sqlite statement to be executed.
            params (tuple[Any, ...] | dict[str, Any] | None, optional): The parameters to be bound
                to the sql statement execution. Defaults to None, not passing any params.

        Returns:
            list[Any]: A list contains all the result entries.
        """
        with self._con as con:
            if params:
                cur = con.execute(sql_stmt, params)
            else:
                cur = con.execute(sql_stmt)
            return cur.fetchall()

    def orm_executemany(
        self,
        sql_stmt: str,
        params: Iterable[tuple[Any, ...] | dict[str, Any]],
    ) -> int:
        """Repeatedly execute the parameterized DML SQL statement sql.

        NOTE that any returning values will be discarded, including with RETURNING stmt.

        Args:
            sql_stmt (str): The sqlite statement to be executed.
            params (Iterable[tuple[Any, ...] | dict[str, Any]]): The set of parameters to be bound
                to the sql statement execution.

        Returns:
            The affected row count.
        """
        with self._con as con:
            cur = con.executemany(sql_stmt, params)
            return cur.rowcount

    def orm_executescript(self, sql_script: str) -> int:
        """Execute one sql script.

        NOTE that any returning values will be discarded, including with RETURNING stmt.

        Args:
            sql_script (str): The sqlite script to be executed.

        Returns:
            The affected row count.
        """
        with self._con as con:
            cur = con.executescript(sql_script)
            return cur.rowcount

    def orm_create_table(
        self,
        *,
        allow_existed: bool = False,
        strict: bool = False,
        without_rowid: bool = False,
    ) -> None:
        """Create the table defined by this ORM with <orm_table_spec>.

        NOTE: strict table option is supported after sqlite3 3.37.

        Args:
            allow_existed (bool, optional): Do not abort on table already created.
                Set True equals to add "IF NOT EXISTS" in the sql statement. Defaults to False.
            strict (bool, optional): Enable strict field type check. Defaults to False.
                See https://www.sqlite.org/stricttables.html for more details.
            without_rowid (bool, optional): Create the table without ROWID. Defaults to False.
                See https://www.sqlite.org/withoutrowid.html for more details.

        Raises:
            sqlite3.DatabaseError on failed sql execution.
        """
        with self._con as con:
            con.execute(
                self.orm_table_spec.table_create_stmt(
                    self.orm_table_name,
                    if_not_exists=allow_existed,
                    strict=strict,
                    without_rowid=without_rowid,
                )
            )

    def orm_create_index(
        self,
        *,
        index_name: str,
        index_keys: tuple[str, ...],
        allow_existed: bool = False,
        unique: bool = False,
    ) -> None:
        """Create index according to the input arguments.

        Args:
            index_name (str): The name of the index.
            index_keys (tuple[str, ...]): The columns for the index.
            allow_existed (bool, optional): Not abort on index already created. Defaults to False.
            unique (bool, optional): Not allow duplicated entries in the index. Defaults to False.

        Raises:
            sqlite3.DatabaseError on failed sql execution.
        """
        index_create_stmt = self.orm_table_spec.table_create_index_stmt(
            table_name=self.orm_table_name,
            index_name=index_name,
            unique=unique,
            if_not_exists=allow_existed,
            index_cols=index_keys,
        )
        with self._con as con:
            con.execute(index_create_stmt)

    def orm_select_entries(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION], ...] | None = None,
        _limit: int | None = None,
        _row_factory: RowFactoryType | None = None,
        **col_values: Any,
    ) -> Generator[TableSpecType | Any]:
        """Select entries from the table accordingly.

        Args:
            _distinct (bool, optional): Deduplicate and only return unique entries. Defaults to False.
            _order_by (tuple[str  |  tuple[str, ORDER_DIRECTION], ...] | None, optional):
                Order the result accordingly. Defaults to None, not sorting the result.
            _limit (int | None, optional): Limit the number of result entries. Defaults to None.
            _row_factory (RowFactoryType | None, optional): By default ORMBase will use <table_spec>.table_row_factory
                as row factory, set this argument to use different row factory. Defaults to None.

        Raises:
            sqlite3.DatabaseError on failed sql execution.

        Yields:
            Generator[TableSpecType, None, None]: A generator that can be used to yield entry from result.
        """
        table_select_stmt = self.orm_table_spec.table_select_stmt(
            select_from=self.orm_table_name,
            distinct=_distinct,
            order_by=_order_by,
            limit=_limit,
            where_cols=tuple(col_values),
        )

        with self._con as con:
            _cur = con.execute(table_select_stmt, col_values)
            if _row_factory is not None:
                _cur.row_factory = _row_factory
            yield from _cur

    def orm_select_entry(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION], ...] | None = None,
        _row_factory: RowFactoryType | None = None,
        **col_values: Any,
    ) -> TableSpecType | Any | None:
        """Select exactly one entry from the table accordingly.

        NOTE that if the select result contains more than one entry, this method will return
            the FIRST one from the result with fetchone API.

        Args:
            _distinct (bool, optional): Deduplicate and only return unique entries. Defaults to False.
            _order_by (tuple[str  |  tuple[str, ORDER_DIRECTION], ...] | None, optional):
                Order the result accordingly. Defaults to None, not sorting the result.
            _row_factory (RowFactoryType | None, optional): By default ORMBase will use <table_spec>.table_row_factory
                as row factory, set this argument to use different row factory. Defaults to None.

        Raises:
            sqlite3.DatabaseError on failed sql execution.

        Returns:
            Exactly one <TableSpecType> entry, or None if not hit.
        """
        table_select_stmt = self.orm_table_spec.table_select_stmt(
            select_from=self.orm_table_name,
            distinct=_distinct,
            order_by=_order_by,
            limit=1,
            where_cols=tuple(col_values),
        )

        with self._con as con:
            _cur = con.execute(table_select_stmt, col_values)
            if _row_factory is not None:
                _cur.row_factory = _row_factory
            return _cur.fetchone()

    def orm_insert_entries(
        self, _in: Iterable[TableSpecType], *, or_option: INSERT_OR | None = None
    ) -> int:
        """Insert entry/entries into this table.

        Args:
            _in (Iterable[TableSpecType]): A list of entries to insert.

        Raises:
            ValueError: On invalid types of _in.
            sqlite3.DatabaseError: On failed sql execution.

        Returns:
            int: Number of inserted entries.
        """
        insert_stmt = self.orm_table_spec.table_insert_stmt(
            insert_into=self.orm_table_name,
            or_option=or_option,
        )
        with self._con as con:
            _cur = con.executemany(
                insert_stmt, (_row.table_dump_asdict() for _row in _in)
            )
            return _cur.rowcount

    def orm_insert_entry(
        self, _in: TableSpecType, *, or_option: INSERT_OR | None = None
    ) -> int:
        """Insert exactly one entry into this table.

        Args:
            _in (TableSpecType): The instance of entry to insert.

        Raises:
            ValueError: On invalid types of _in.
            sqlite3.DatabaseError: On failed sql execution.

        Returns:
            int: Number of inserted entries. In normal case it should be 1.
        """
        insert_stmt = self.orm_table_spec.table_insert_stmt(
            insert_into=self.orm_table_name,
            or_option=or_option,
        )
        with self._con as con:
            _cur = con.execute(insert_stmt, _in.table_dump_asdict())
            return _cur.rowcount

    def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION]] | None = None,
        _limit: int | None = None,
        _row_factory: RowFactoryType | None = None,
        **cols_value: Any,
    ) -> int:
        """Delete entries from the table accordingly.

        Args:
            _order_by (tuple[str  |  tuple[str, ORDER_DIRECTION]] | None, optional): Order the matching entries
                before executing the deletion, used together with <_limit>. Defaults to None.
            _limit (int | None, optional): Only delete <_limit> number of entries. Defaults to None.
            _row_factory (RowFactoryType | None, optional): By default ORMBase will use <table_spec>.table_row_factory
                as row factory, set this argument to use different row factory. Defaults to None.

        Returns:
            int: The num of entries deleted.
        """
        delete_stmt = self.orm_table_spec.table_delete_stmt(
            delete_from=self.orm_table_name,
            limit=_limit,
            order_by=_order_by,
            returning_cols=None,
            where_cols=tuple(cols_value),
        )

        with self._con as con:
            _cur = con.execute(delete_stmt, cols_value)
            if _row_factory:
                _cur.row_factory = _row_factory
            return _cur.rowcount

    def orm_delete_entries_with_returning(
        self,
        *,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION]] | None = None,
        _limit: int | None = None,
        _returning_cols: tuple[str, ...] | Literal["*"],
        _row_factory: RowFactoryType | None = None,
        **cols_value: Any,
    ) -> Generator[TableSpecType]:
        """Delete entries from the table accordingly.

        NOTE that only sqlite3 version >= 3.35 supports returning statement.

        Args:
            _order_by (tuple[str  |  tuple[str, ORDER_DIRECTION]] | None, optional): Order the matching entries
                before executing the deletion, used together with <_limit>. Defaults to None.
            _limit (int | None, optional): Only delete <_limit> number of entries. Defaults to None.
            _returning_cols (tuple[str, ...] | Literal["*"] ): Return the deleted entries on execution.
            _row_factory (RowFactoryType | None, optional): By default ORMBase will use <table_spec>.table_row_factory
                as row factory, set this argument to use different row factory. Defaults to None.

        Returns:
            Generator[TableSpecType, None, None]: If <_returning_cols> is defined, returns a generator which can
                be used to yield the deleted entries from.
        """
        delete_stmt = self.orm_table_spec.table_delete_stmt(
            delete_from=self.orm_table_name,
            limit=_limit,
            order_by=_order_by,
            returning_cols=_returning_cols,
            where_cols=tuple(cols_value),
        )

        def _gen():
            with self._con as con:
                _cur = con.execute(delete_stmt, cols_value)
                if _row_factory is not None:
                    _cur.row_factory = _row_factory
                yield from _cur

        return _gen()

    def orm_select_all_with_pagination(
        self, *, batch_size: int
    ) -> Generator[TableSpecType | Any]:
        """Select all entries from the table accordingly with pagination.

        This is implemented by seek with rowid, so it will not work on without_rowid table.

        Args:
            batch_size (int): The entry number for each page.

        Raises:
            ValueError on invalid batch_size.
            sqlite3.DatabaseError on failed sql execution.

        Yields:
            Generator[TableSpecType, None, None]: A generator that can be used to yield entry from result.
        """
        if batch_size < 0:
            raise ValueError("batch_size must be positive integer")

        _sql_stmt = self.orm_table_spec.table_select_stmt(
            select_cols="rowid,*",
            select_from=self.orm_table_name,
            where_stmt="WHERE rowid > :not_before",
            limit=batch_size,
        )
        _tuple_factory = self.orm_table_spec.table_from_tuple

        not_before = 0
        for _ in count():
            with self._con as con:
                _cur = con.execute(_sql_stmt, {"not_before": not_before})
                _cur.row_factory = None

                rowid = -1
                _row_tuple: tuple[int, ...]
                for _row_tuple in _cur:
                    rowid, *_raw_entry = _row_tuple
                    yield _tuple_factory(_raw_entry)

                if rowid < 0:
                    return
                not_before = rowid

    def orm_check_entry_exist(self, **cols: Any) -> bool:
        """A quick method to check whether entry(entries) indicated by cols exists.

        This method uses COUNT function to count the selected entry.

        Args:
            **cols: cols pair to locate the entry(entries).

        Returns:
            Returns True if at least one entry matches the input cols exists, otherwise False.
        """
        _stmt = self.orm_table_spec.table_select_stmt(
            select_from=self.orm_table_name,
            select_cols="*",
            function="count",
            where_cols=tuple(cols),
        )

        with self._con as con:
            _cur = con.execute(_stmt, cols)
            _cur.row_factory = None  # bypass con scope row_factory
            _res: tuple[int] = _cur.fetchone()
            return _res[0] > 0


ORMBaseType = TypeVar("ORMBaseType", bound=ORMBase)
