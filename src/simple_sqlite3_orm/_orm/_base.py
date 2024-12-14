from __future__ import annotations

import sqlite3
import sys
from functools import cached_property
from itertools import count
from typing import (
    TYPE_CHECKING,
    Any,
    Generator,
    Generic,
    Iterable,
    Literal,
    TypeVar,
    overload,
)
from weakref import WeakValueDictionary

from typing_extensions import ParamSpec

from simple_sqlite3_orm._sqlite_spec import INSERT_OR, ORDER_DIRECTION
from simple_sqlite3_orm._table_spec import TableSpec, TableSpecType
from simple_sqlite3_orm._types import RowFactoryType

_parameterized_orm_cache: WeakValueDictionary[
    tuple[type[ORMBase], type[TableSpec]], type[ORMBase[Any]]
] = WeakValueDictionary()


P = ParamSpec("P")
RT = TypeVar("RT")

if sys.version_info >= (3, 9):
    from types import GenericAlias as _GenericAlias
else:
    from typing import List

    if not TYPE_CHECKING:
        _GenericAlias = type(List[int])
    else:

        class _GenericAlias(type(List)):
            def __new__(
                cls, _type: type[Any], _params: type[Any] | tuple[type[Any], ...]
            ):
                """For type check only, typing the _GenericAlias as GenericAlias."""


class ORMBase(Generic[TableSpecType]):
    """ORM layer for <TableSpecType>.

    NOTE that ORMBase will set the connection scope row_factory to <tablespec>'s table_row_factory.
        See TableSpec.table_row_factory for more details.

    NOTE that instance of ORMBase cannot be used in multi-threaded environment as the underlying
        sqlite3 connection. Use ORMThreadPoolBase for multi-threaded environment. For asyncio,
        use AsyncORMThreadPoolBase.

    The underlying connection can be used in multiple connection for accessing different table in
        the connected database.

    Attributes:
        con (sqlite3.Connection): The sqlite3 connection used by this ORM.
        table_name (str): The name of the table in the database <con> connected to.
        schema_name (str): the schema of the table if multiple databases are attached to <con>.
    """

    orm_table_spec: type[TableSpecType]

    def __init__(
        self,
        con: sqlite3.Connection,
        table_name: str,
        schema_name: str | Literal["temp"] | None = None,
    ) -> None:
        self._table_name = table_name
        self._schema_name = schema_name
        self._con = con
        con.row_factory = self.orm_table_spec.table_row_factory

    def __class_getitem__(cls, params: Any | type[Any] | type[TableSpecType]) -> Any:
        # just for convienience, passthrough anything that is not type[TableSpecType]
        #   to Generic's __class_getitem__ and return it.
        # Typically this is for subscript ORMBase with TypeVar or another Generic.
        if not (isinstance(params, type) and issubclass(params, TableSpec)):
            return super().__class_getitem__(params)  # type: ignore

        key = (cls, params)
        if _cached_type := _parameterized_orm_cache.get(key):
            return _GenericAlias(_cached_type, params)

        new_parameterized_ormbase: type[ORMBase] = type(
            f"{cls.__name__}[{params.__name__}]", (cls,), {}
        )
        new_parameterized_ormbase.orm_table_spec = params  # type: ignore
        _parameterized_orm_cache[key] = new_parameterized_ormbase
        return _GenericAlias(new_parameterized_ormbase, params)

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
            f"{self._schema_name}.{self._table_name}"
            if self._schema_name
            else self._table_name
        )

    def orm_execute(
        self, sql_stmt: str, params: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> list[Any]:
        """Execute one sql statement and get the all the result.

        The result will be fetched with fetchall API and returned as it.

        This method is inteneded for executing simple sql_stmt with small result.
        For complicated sql statement and large result, please use sqlite3.Connection object
            exposed by orm_con and manipulate the Cursor object by yourselves.

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

    @overload
    def orm_select_entries(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION], ...] | None = None,
        _limit: int | None = None,
        _row_factory: RowFactoryType,
        **col_values: Any,
    ) -> Generator[Any, None, None]: ...

    @overload
    def orm_select_entries(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION], ...] | None = None,
        _limit: int | None = None,
        _row_factory: None = None,
        **col_values: Any,
    ) -> Generator[TableSpecType, None, None]: ...

    def orm_select_entries(
        self,
        *,
        _distinct: bool = False,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION], ...] | None = None,
        _limit: int | None = None,
        _row_factory: RowFactoryType | None = None,
        **col_values: Any,
    ) -> Generator[TableSpecType, None, None]:
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

    @overload
    def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION]] | None = None,
        _limit: int | None = None,
        _returning_cols: None = None,
        _row_factory: None = None,
        **cols_value: Any,
    ) -> int: ...

    @overload
    def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION]] | None = None,
        _limit: int | None = None,
        _returning_cols: tuple[str, ...] | Literal["*"],
        _row_factory: None = None,
        **cols_value: Any,
    ) -> Generator[TableSpecType, None, None]: ...

    @overload
    def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION]] | None = None,
        _limit: int | None = None,
        _returning_cols: tuple[str, ...] | Literal["*"],
        _row_factory: RowFactoryType,
        **cols_value: Any,
    ) -> Generator[Any, None, None]: ...

    def orm_delete_entries(
        self,
        *,
        _order_by: tuple[str | tuple[str, ORDER_DIRECTION]] | None = None,
        _limit: int | None = None,
        _returning_cols: tuple[str, ...] | Literal["*"] | None = None,
        _row_factory: RowFactoryType | None = None,
        **cols_value: Any,
    ) -> int | Generator[TableSpecType, None, None]:
        """Delete entries from the table accordingly.

        Args:
            _order_by (tuple[str  |  tuple[str, ORDER_DIRECTION]] | None, optional): Order the matching entries
                before executing the deletion, used together with <_limit>. Defaults to None.
            _limit (int | None, optional): Only delete <_limit> number of entries. Defaults to None.
            _returning_cols (tuple[str, ...] | Literal[, optional): Return the deleted entries on execution.
                NOTE that only sqlite3 version >= 3.35 supports returning statement. Defaults to None.
            _row_factory (RowFactoryType | None, optional): By default ORMBase will use <table_spec>.table_row_factory
                as row factory, set this argument to use different row factory. Defaults to None.

        Returns:
            int: The num of entries deleted.
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

        if _returning_cols:

            def _gen():
                with self._con as con:
                    _cur = con.execute(delete_stmt, cols_value)
                    if _row_factory is not None:
                        _cur.row_factory = _row_factory
                    yield from _cur

            return _gen()

        else:
            with self._con as con:
                _cur = con.execute(delete_stmt, cols_value)
                return _cur.rowcount

    def orm_select_all_with_pagination(
        self, *, batch_size: int
    ) -> Generator[TableSpecType, None, None]:
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
            _cur = con.execute(_stmt)
            _cur.row_factory = None  # bypass con scope row_factory
            _res: tuple[int] = _cur.fetchone()
            return _res[0] > 0


ORMBaseType = TypeVar("ORMBaseType", bound=ORMBase)
