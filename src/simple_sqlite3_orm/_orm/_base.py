from __future__ import annotations

import sqlite3
import warnings
from functools import cached_property, partial
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generator,
    Generic,
    Iterable,
    Literal,
    Mapping,
    TypeVar,
    Union,
    overload,
)

from typing_extensions import ParamSpec, Self

from simple_sqlite3_orm._orm._utils import parameterized_class_getitem
from simple_sqlite3_orm._sqlite_spec import OR_OPTIONS
from simple_sqlite3_orm._table_spec import (
    CreateIndexParams,
    CreateTableParams,
    TableSpec,
    TableSpecType,
)
from simple_sqlite3_orm._typing import (
    ColsDefinition,
    ColsDefinitionWithDirection,
    ConnectionFactoryType,
    RowFactoryType,
)

P = ParamSpec("P")
RT = TypeVar("RT")

DoNotChangeRowFactory = Literal["do_not_change"]
DO_NOT_CHANGE_ROW_FACTORY: DoNotChangeRowFactory = "do_not_change"

RowFactorySpecifier = Union[
    RowFactoryType,
    Literal[
        "sqlite3_row_factory",
        "table_spec",
        "table_spec_no_validation",
    ],
    DoNotChangeRowFactory,
    None,
]
"""Specifiy which row_factory to use.

For each option:
    1. RowFactoryType: specify arbitrary row_factory.
    2. None: clear the connection scope row_factory(set to None).
    3. "sqlite3_row_factory": set to use sqlite3.Row as row_factory.
    4. "table_spec": use TableSpec.table_row_factory as row_factory.
    5. "table_spec_no_validation": use TableSpec.table_row_factory as row_factory, but with validation=False.
    6. "do_not_change": do not change the connection scope row_factory.
"""


def _select_row_factory(
    table_spec: type[TableSpec],
    row_factory_specifier: RowFactorySpecifier,
) -> RowFactoryType | None | DoNotChangeRowFactory:  # pragma: no cover
    """Helper function to get row_factory by row_factory_specifier."""
    if row_factory_specifier is None:
        return None

    if callable(row_factory_specifier):
        return row_factory_specifier
    if row_factory_specifier == "table_spec":
        return table_spec.table_row_factory
    if row_factory_specifier == "table_spec_no_validation":
        return partial(table_spec.table_row_factory, validation=False)
    if row_factory_specifier == "sqlite3_row_factory":
        return sqlite3.Row

    if row_factory_specifier == DO_NOT_CHANGE_ROW_FACTORY:
        return DO_NOT_CHANGE_ROW_FACTORY
    raise ValueError(f"invalid specifier: {row_factory_specifier}")


def _merge_iters(
    _left: Iterable[Mapping[str, Any]], _right: Iterable[Mapping[str, Any]]
) -> Generator[dict[str, Any]]:
    """Merge two iterables of Mappings into one iterable of dict."""
    for _entry_l, _entry_r in zip(_left, _right):
        yield dict(**_entry_l, **_entry_r)


class ORMCommonBase(Generic[TableSpecType]):
    orm_table_spec: type[TableSpecType]

    #
    # ------------ orm_bootstrap APIs ------------ #
    #
    if not TYPE_CHECKING:
        _orm_table_name: str
        """
        Used by ORM internally, should not be set directly.

        Directly setting this variable is DEPRECATED, use orm_bootstrap_table_name instead.
        """

    orm_bootstrap_table_name: str
    orm_bootstrap_create_table_params: str | CreateTableParams
    orm_bootstrap_indexes_params: list[str | CreateIndexParams]

    def __init_subclass__(cls, **kwargs) -> None:
        # check this class' dict to only get the name set during this subclass' creation
        _set_table_name = cls.__dict__.get("orm_bootstrap_table_name")

        if _deprecated_set_table_name := cls.__dict__.get("_orm_table_name"):
            warnings.warn(
                "Directly setting this variable is DEPRECATED, use orm_bootstrap_table_name instead",
                stacklevel=1,
            )
            # For backward compatibility, still use the _orm_table_name if set in class creation namespace
            if not _set_table_name:
                _set_table_name = _deprecated_set_table_name

        # only override the _orm_table_name for the subclass when orm_bootstrap_table_name is set
        #   in the class creation namespace.
        if _set_table_name:
            cls._orm_table_name = _set_table_name


class ORMBase(ORMCommonBase[TableSpecType]):
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
            table_name specified by orm_bootstrap_table_name attr to allow using different table_name for just one connection.
        schema_name (str): The schema of the table if multiple databases are attached to <con>.
        row_factory (RowFactorySpecifier): The connection scope row_factory to use. Default to "table_sepc".
    """

    def orm_bootstrap_db(self) -> None:
        """Bootstrap the database this ORM connected to.

        This method will refer to the following attrs to setup table and indexes:
        1. orm_bootstrap_table_name: the name of table to be created.
        2. orm_bootstrap_create_table_params: the sqlite query to create the table,
            it can be provided as sqlite query, or CreateTableParams for table_create_stmt
            to generate sqlite query from.
            It not specified, the table create statement will be generated with default configs,
            See table_spec.table_create_stmt method for more details.
        3. orm_bootstrap_indexes_params: optional, a list of sqlite query or
            CreateIndexParams(for table_create_index_stmt to generate sqlite query from) to
            create indexes from.

        NOTE that ORM will not know whether the connected database has already been
            bootstrapped or not, this is up to caller to check.
        """
        _table_name = self.orm_table_name

        _table_create_stmt = getattr(self, "orm_bootstrap_create_table_params", None)
        if not _table_create_stmt:
            _table_create_stmt = self.orm_table_spec.table_create_stmt(_table_name)
        elif isinstance(_table_create_stmt, dict):
            _table_create_stmt = self.orm_table_spec.table_create_stmt(
                _table_name,
                **_table_create_stmt,
            )
        with self._con as conn:
            conn.execute(_table_create_stmt)

        if _index_stmts := getattr(self, "orm_bootstrap_indexes_params", None):
            for _index_stmt in _index_stmts:
                if isinstance(_index_stmt, dict):
                    _index_stmt = self.orm_table_spec.table_create_index_stmt(
                        table_name=_table_name,
                        **_index_stmt,
                    )
                with self._con as conn:
                    conn.execute(_index_stmt)

    def __init__(
        self,
        con: sqlite3.Connection | ConnectionFactoryType,
        table_name: str | None = None,
        schema_name: str | Literal["temp"] | None = None,
        *,
        row_factory: RowFactorySpecifier = "table_spec",
    ) -> None:
        if table_name:
            # the table_name passed in by keyword arg has higher priority than the one set
            #   by class variable.
            self._orm_table_name = table_name

        if not getattr(self, "_orm_table_name", None):
            raise ValueError(
                "table_name must be provided either by class variable orm_bootstrap_table_name, "
                "or by providing <table_name> keyword arg"
            )

        self._schema_name = schema_name

        if isinstance(con, sqlite3.Connection):
            self._con = con
        elif callable(con) and isinstance(_conn := con(), sqlite3.Connection):
            self._con = _conn
        else:
            raise ValueError(f"invalid {con=}")

        _row_factory = _select_row_factory(self.orm_table_spec, row_factory)
        if _row_factory != DO_NOT_CHANGE_ROW_FACTORY:
            self._con.row_factory = _row_factory

    __class_getitem__ = classmethod(parameterized_class_getitem)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exec_type, exc_val, exc_tb):
        self._con.close()
        return False

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

    @property
    def orm_conn_row_factory(self) -> RowFactoryType | None:
        """Get and set the connection scope row_factory for this ORM instance."""
        return self._con.row_factory

    @orm_conn_row_factory.setter
    def orm_conn_row_factory(self, _row_factory: RowFactoryType | None) -> None:
        self._con.row_factory = _row_factory

    @overload
    def orm_execute(
        self,
        sql_stmt: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
        *,
        row_factory: Callable[[sqlite3.Cursor, Any], RT],
    ) -> list[RT]: ...

    @overload
    def orm_execute(
        self,
        sql_stmt: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
        *,
        row_factory: DoNotChangeRowFactory = DO_NOT_CHANGE_ROW_FACTORY,
    ) -> list[TableSpecType]: ...

    @overload
    def orm_execute(
        self,
        sql_stmt: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
        *,
        row_factory: None,
    ) -> list[tuple[Any, ...]]: ...

    def orm_execute(
        self,
        sql_stmt: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
        *,
        row_factory: RowFactorySpecifier
        | DoNotChangeRowFactory = DO_NOT_CHANGE_ROW_FACTORY,
    ) -> list[Any]:
        """Execute one sql statement and get the all the result.

        NOTE that caller needs to serialize the `params` before hands if needed, `orm_execute` will
            not do any processing over the input `params`, and just use as it for execution.
        The result will be fetched with fetchall API and returned as it.

        Args:
            sql_stmt (str): The sqlite statement to be executed.
            params (tuple[Any, ...] | dict[str, Any] | None, optional): The parameters to be bound
                to the sql statement execution. Defaults to None, not passing any params.
            row_factory (RowFactorySpecifier | DoNotChangeRowFactory, optional): specify to use
                different row_factory for the query. Default to not change the current row_factory.
                NOTE that None value here means unset the row_factory for this query.

        Returns:
            list[Any]: A list contains all the result entries.
        """
        with self._con as con:
            if params:
                cur = con.execute(sql_stmt, params)
            else:
                cur = con.execute(sql_stmt)

            if row_factory != DO_NOT_CHANGE_ROW_FACTORY:
                cur.row_factory = _select_row_factory(self.orm_table_spec, row_factory)  # type: ignore
            return cur.fetchall()

    @overload
    def orm_execute_gen(
        self,
        sql_stmt: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
        *,
        row_factory: Callable[[sqlite3.Cursor, Any], RT],
    ) -> Generator[RT]: ...

    @overload
    def orm_execute_gen(
        self,
        sql_stmt: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
        *,
        row_factory: DoNotChangeRowFactory = DO_NOT_CHANGE_ROW_FACTORY,
    ) -> Generator[TableSpecType]: ...

    @overload
    def orm_execute_gen(
        self,
        sql_stmt: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
        *,
        row_factory: None,
    ) -> Generator[tuple[Any, ...]]: ...

    def orm_execute_gen(
        self,
        sql_stmt: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
        *,
        row_factory: RowFactorySpecifier
        | DoNotChangeRowFactory = DO_NOT_CHANGE_ROW_FACTORY,
    ) -> Generator[Any]:
        """The same as orm_execute, but as a Generator."""
        with self._con as con:
            if params:
                cur = con.execute(sql_stmt, params)
            else:
                cur = con.execute(sql_stmt)

            if row_factory != DO_NOT_CHANGE_ROW_FACTORY:
                cur.row_factory = _select_row_factory(self.orm_table_spec, row_factory)  # type: ignore
            yield from cur

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
        _stmt: str | None = None,
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
            _stmt (str, optional): If provided, all params will be ignored and query statement will not
                be generated with the params, instead the provided <_stmt> will be used as query statement.

        Raises:
            sqlite3.DatabaseError on failed sql execution.
        """
        if not _stmt:
            _stmt = self.orm_table_spec.table_create_stmt(
                self.orm_table_name,
                if_not_exists=allow_existed,
                strict=strict,
                without_rowid=without_rowid,
            )
        with self._con as con:
            con.execute(_stmt)

    def orm_create_index(
        self,
        *,
        index_name: str,
        index_keys: ColsDefinition | ColsDefinitionWithDirection,
        allow_existed: bool = False,
        unique: bool = False,
        _stmt: str | None = None,
    ) -> None:
        """Create index according to the input arguments.

        Args:
            index_name (str): The name of the index.
            index_keys (ColsDefinition | ColsDefinitionWithDirection): The columns for the index.
            allow_existed (bool, optional): Not abort on index already created. Defaults to False.
            unique (bool, optional): Not allow duplicated entries in the index. Defaults to False.
            _stmt (str, optional): If provided, all params will be ignored and query statement will not
                be generated with the params, instead the provided <_stmt> will be used as query statement.

        Raises:
            sqlite3.DatabaseError on failed sql execution.
        """
        if not _stmt:
            _stmt = self.orm_table_spec.table_create_index_stmt(
                table_name=self.orm_table_name,
                index_name=index_name,
                unique=unique,
                if_not_exists=allow_existed,
                index_cols=index_keys,
            )
        with self._con as con:
            con.execute(_stmt)

    @overload
    def orm_select_entries(
        self,
        col_value_pairs: Mapping[str, Any] | None = None,
        *,
        _distinct: bool = False,
        _order_by: ColsDefinition | ColsDefinitionWithDirection | None = None,
        _limit: int | None = None,
        _row_factory: Callable[[sqlite3.Cursor, Any], RT],
        _stmt: str | None = None,
        **col_values: Any,
    ) -> Generator[RT]: ...

    @overload
    def orm_select_entries(
        self,
        col_value_pairs: Mapping[str, Any] | None = None,
        *,
        _distinct: bool = False,
        _order_by: ColsDefinition | ColsDefinitionWithDirection | None = None,
        _limit: int | None = None,
        _row_factory: None = None,
        _stmt: str | None = None,
        **col_values: Any,
    ) -> Generator[TableSpecType | Any]: ...

    def orm_select_entries(
        self,
        col_value_pairs: Mapping[str, Any] | None = None,
        *,
        _distinct: bool = False,
        _order_by: ColsDefinition | ColsDefinitionWithDirection | None = None,
        _limit: int | None = None,
        _row_factory: RowFactoryType | None = None,
        _stmt: str | None = None,
        **col_values: Any,
    ) -> Generator[TableSpecType | Any]:
        """Select entries from the table accordingly.

        Args:
            col_value_pairs(Mapping[str, Any] | None): provide col/value pairs by a Mapping, if provided,
                the pairs in this mapping will take prior than the one specified in <col_values>.
            _distinct (bool, optional): Deduplicate and only return unique entries. Defaults to False.
            _order_by (ColsDefinition | ColsDefinitionWithDirection | None, optional):
                Order the result accordingly. Defaults to None, not sorting the result.
            _limit (int | None, optional): Limit the number of result entries. Defaults to None.
            _row_factory (RowFactoryType | None, optional): Set to use different row factory for this query.
                Defaults to None(do not change row_factory).
            _stmt (str, optional): If provided, all params will be ignored and query statement will not
                be generated with the params, instead the provided <_stmt> will be used as query statement.
            **col_values: provide col/value pairs by kwargs. Col/value pairs in <col_values> have lower priority over
                the one specified by <_col_vlues_dict>.

        Raises:
            sqlite3.DatabaseError on failed sql execution.

        Yields:
            Generator[TableSpecType | Any]: A generator that can be used to yield entry from result.
        """
        _table_spec = self.orm_table_spec
        if col_value_pairs:
            col_values.update(col_value_pairs)
        col_values = _table_spec.table_serialize_mapping(col_values)

        if not _stmt:
            _stmt = _table_spec.table_select_stmt(
                select_from=self.orm_table_name,
                distinct=_distinct,
                order_by=_order_by,
                limit=_limit,
                where_cols=tuple(col_values),
            )

        with self._con as con:
            _cur = con.execute(_stmt, col_values)
            if _row_factory is not None:
                _cur.row_factory = _row_factory  # type: ignore
            yield from _cur

    @overload
    def orm_select_entry(
        self,
        col_value_pairs: Mapping[str, Any] | None = None,
        *,
        _distinct: bool = False,
        _order_by: ColsDefinition | ColsDefinitionWithDirection | None = None,
        _row_factory: Callable[[sqlite3.Cursor, Any], RT],
        _stmt: str | None = None,
        **col_values: Any,
    ) -> RT: ...

    @overload
    def orm_select_entry(
        self,
        col_value_pairs: Mapping[str, Any] | None = None,
        *,
        _distinct: bool = False,
        _order_by: ColsDefinition | ColsDefinitionWithDirection | None = None,
        _row_factory: None = None,
        _stmt: str | None = None,
        **col_values: Any,
    ) -> TableSpecType | Any: ...

    def orm_select_entry(
        self,
        col_value_pairs: Mapping[str, Any] | None = None,
        *,
        _distinct: bool = False,
        _order_by: ColsDefinition | ColsDefinitionWithDirection | None = None,
        _row_factory: RowFactoryType | None = None,
        _stmt: str | None = None,
        **col_values: Any,
    ) -> TableSpecType | Any | None:
        """Select exactly one entry from the table accordingly.

        NOTE that if the select result contains more than one entry, this method will return
            the FIRST one from the result with fetchone API.

        Args:
            col_value_pairs(Mapping[str, Any] | None): provide col/value pairs by a Mapping, if provided,
                the pairs in this mapping will take prior than the one specified in <col_values>.
            _distinct (bool, optional): Deduplicate and only return unique entries. Defaults to False.
            _order_by (ColsDefinition | ColsDefinitionWithDirection | None, optional):
                Order the result accordingly. Defaults to None, not sorting the result.
            _row_factory (RowFactoryType | None, optional): Set to use different row factory for this query.
                Defaults to None(do not change row_factory).
            _stmt (str, optional): If provided, all params will be ignored and query statement will not
                be generated with the params, instead the provided <_stmt> will be used as query statement.
            **col_values: provide col/value pairs by kwargs. Col/value pairs in <col_values> have lower priority over
                the one specified by <_col_vlues_dict>.

        Raises:
            sqlite3.DatabaseError on failed sql execution.

        Returns:
            TableSpecType | Any: Exactly one entry, or None if not hit.
        """
        _table_spec = self.orm_table_spec
        if col_value_pairs:
            col_values.update(col_value_pairs)
        col_values = _table_spec.table_serialize_mapping(col_values)

        if not _stmt:
            _stmt = _table_spec.table_select_stmt(
                select_from=self.orm_table_name,
                distinct=_distinct,
                order_by=_order_by,
                limit=1,
                where_cols=tuple(col_values),
            )

        with self._con as con:
            _cur = con.execute(_stmt, col_values)
            if _row_factory is not None:
                _cur.row_factory = _row_factory  # type: ignore
            return _cur.fetchone()

    def orm_insert_entries(
        self,
        _in: Iterable[TableSpecType],
        *,
        or_option: OR_OPTIONS | None = None,
        _stmt: str | None = None,
    ) -> int:
        """Insert an iterable of rows represented as TableSpec insts into this table.

        Args:
            _in (Iterable[TableSpecType]): An iterable of rows as TableSpec insts to insert.
            or_option (INSERT_OR | None, optional): The fallback operation if insert failed. Defaults to None.
            _stmt (str, optional): If provided, all params will be ignored and query statement will not
                be generated with the params, instead the provided <_stmt> will be used as query statement.

        Raises:
            ValueError: On invalid types of _in.
            sqlite3.DatabaseError: On failed sql execution.

        Returns:
            int: Number of inserted entries.
        """
        _table_spec = self.orm_table_spec
        if not _stmt:
            _stmt = _table_spec.table_insert_stmt(
                insert_into=self.orm_table_name,
                or_option=or_option,
            )
        with self._con as con:
            _cur = con.executemany(_stmt, (entry.table_dump_asdict() for entry in _in))
            return _cur.rowcount

    def orm_insert_mappings(
        self,
        _in: Iterable[Mapping[str, Any]],
        *,
        or_option: OR_OPTIONS | None = None,
        _stmt: str | None = None,
    ) -> int:
        """Insert an iterable of rows represented as mappings into this table.

        Each mapping stores cols with values in application types. Assuming that all entries in
            this Iterable contains mapping with the same schema.

        Args:
            _in (Iterable[Mapping[str, Any]]): An iterable of mappings to insert.
            or_option (INSERT_OR | None, optional): The fallback operation if insert failed. Defaults to None.
            _stmt (str, optional): If provided, all params will be ignored and query statement will not
                be generated with the params, instead the provided <_stmt> will be used as query statement.

        Raises:
            ValueError: On invalid types of _in.
            sqlite3.DatabaseError: On failed sql execution.

        Returns:
            int: Number of inserted entries.
        """
        _in_iter = iter(_in)
        try:
            _first_entry = next(_in_iter)
        except StopIteration:
            return 0

        _table_spec = self.orm_table_spec
        if not _stmt:
            _stmt = _table_spec.table_insert_stmt(
                insert_into=self.orm_table_name,
                or_option=or_option,
                insert_cols=tuple(_first_entry),
            )

        with self._con as con:
            _cur = con.executemany(
                _stmt,
                (
                    _table_spec.table_serialize_mapping(entry)
                    for entry in chain([_first_entry], _in_iter)
                ),
            )
            return _cur.rowcount

    def orm_insert_entry(
        self,
        _in: TableSpecType,
        *,
        or_option: OR_OPTIONS | None = None,
        _stmt: str | None = None,
    ) -> int:
        """Insert exactly one entry into this table.

        Args:
            _in (TableSpecType): The instance of entry to insert.
            _stmt (str, optional): If provided, all params will be ignored and query statement will not
                be generated with the params, instead the provided <_stmt> will be used as query statement.

        Raises:
            ValueError: On invalid types of _in.
            sqlite3.DatabaseError: On failed sql execution.

        Returns:
            int: Number of inserted entries. In normal case it should be 1.
        """
        _table_spec = self.orm_table_spec
        if not _stmt:
            _stmt = _table_spec.table_insert_stmt(
                insert_into=self.orm_table_name, or_option=or_option
            )

        with self._con as con:
            _cur = con.execute(_stmt, _in.table_dump_asdict())
            return _cur.rowcount

    def orm_insert_mapping(
        self,
        _in: Mapping[str, Any],
        *,
        or_option: OR_OPTIONS | None = None,
        _stmt: str | None = None,
    ) -> int:
        """Insert exactly one entry(represented as a mapping) into this table.

        Args:
            _in (TableSpecType): The instance of entry to insert.
            _stmt (str, optional): If provided, all params will be ignored and query statement will not
                be generated with the params, instead the provided <_stmt> will be used as query statement.

        Raises:
            ValueError: On invalid types of _in.
            sqlite3.DatabaseError: On failed sql execution.

        Returns:
            int: Number of inserted entries. In normal case it should be 1.
        """
        _table_spec = self.orm_table_spec
        if not _stmt:
            _stmt = _table_spec.table_insert_stmt(
                insert_into=self.orm_table_name,
                or_option=or_option,
                insert_cols=tuple(_in),
            )

        with self._con as con:
            _cur = con.execute(_stmt, _table_spec.table_serialize_mapping(_in))
            return _cur.rowcount

    def orm_update_entries(
        self,
        *,
        set_values: Mapping[str, Any] | TableSpec,
        where_cols_value: Mapping[str, Any] | None = None,
        where_stmt: str | None = None,
        or_option: OR_OPTIONS | None = None,
        _extra_params: Mapping[str, Any] | None = None,
        _stmt: str | None = None,
    ) -> int:
        """UPDATE specific entries by matching <where_cols_value>.

        NOTE: if you want to using the same query stmt with different set of params(set col/values and/or where col/values),
            it is highly recommended to use `orm_update_entries_many` API, it will be significantly faster to call `orm_update_entries`
            in a for loop(in my test with 2000 entries, using `orm_update_entries_many` is around 300 times faster).
        NOTE: currently UPDATE-WITH-LIMIT and RETURNING are not supported by this method.

        Args:
            set_values (Mapping[str, Any] | TableSpec): values to update.
            where_cols_value (Mapping[str, Any], optional): cols to matching. This method will
                also generate WHERE statement of matching each cols specified in this mapping.
            where_stmt (str | None, optional): directly provide WHERE statement. If provided,
                <where_cols_value> will only be used as params.
            or_option (OR_OPTIONS | None, optional): specify the operation if UPDATE failed.
            _extra_params (Mapping[str, Any] | None, optional): provide extra named params
                for sqlite3 query execution. NOTE that `_extra_params` takes higher priority
                to any named params specified by `where_cols_value` and `set_values`. Defaults to None.
            _stmt (str | None, optional): directly provide the UPDATE query, if provided,
                <where_cols_value>, <where_stmt> and <or_option> will be ignored.

        Raises:
            SQLite3 DB Errors on failed operations.

        Returns:
            Affected rows count.
        """
        _table_spec = self.orm_table_spec

        if isinstance(set_values, _table_spec):
            _serialized_set_values = set_values.model_dump()
        elif isinstance(set_values, Mapping):
            _serialized_set_values = _table_spec.table_serialize_mapping(set_values)
        else:  # pragma: no cover
            raise ValueError(f"unexpected {type(set_values)=}")

        _serialized_where_col_values = {}
        if where_cols_value:
            _serialized_where_col_values = _table_spec.table_preprare_update_where_cols(
                _table_spec.table_serialize_mapping(where_cols_value)
            )

        if not _stmt:
            _extra_update_stmt_params: dict[str, Any] = {}
            if where_stmt:
                _extra_update_stmt_params = dict(where_stmt=where_stmt)
            elif where_cols_value:
                _extra_update_stmt_params = dict(where_cols=tuple(where_cols_value))

            _stmt = _table_spec.table_update_stmt(
                or_option=or_option,
                update_target=self.orm_table_name,
                set_cols=tuple(_serialized_set_values),
                **_extra_update_stmt_params,
            )

        _params = dict(**_serialized_set_values, **_serialized_where_col_values)
        if _extra_params:
            _params.update(_extra_params)
        with self.orm_con as con:
            _cur = con.execute(_stmt, _params)
            return _cur.rowcount

    @overload
    def orm_update_entries_many(
        self,
        *,
        set_cols: tuple[str, ...],
        where_cols: tuple[str, ...] | None = None,
        where_stmt: str | None = None,
        set_cols_value: Iterable[Mapping[str, Any]],
        where_cols_value: Iterable[Mapping[str, Any]] | None = None,
        or_option: OR_OPTIONS | None = None,
        _extra_params: Mapping[str, Any] | None = None,
        _extra_params_iter: Iterable[Mapping[str, Any]] | None = None,
        _stmt: None = None,
    ) -> int: ...

    @overload
    def orm_update_entries_many(
        self,
        *,
        set_cols: None = None,
        where_cols: None = None,
        where_stmt: None = None,
        set_cols_value: None = None,
        where_cols_value: None = None,
        or_option: None = None,
        _extra_params: Mapping[str, Any] | None = None,
        _extra_params_iter: Iterable[Mapping[str, Any]] | None = None,
        _stmt: str,
    ) -> int: ...

    def orm_update_entries_many(
        self,
        *,
        set_cols: tuple[str, ...] | None = None,
        where_cols: tuple[str, ...] | None = None,
        where_stmt: str | None = None,
        set_cols_value: Iterable[Mapping[str, Any]] | None = None,
        where_cols_value: Iterable[Mapping[str, Any]] | None = None,
        or_option: OR_OPTIONS | None = None,
        _extra_params: Mapping[str, Any] | None = None,
        _extra_params_iter: Iterable[Mapping[str, Any]] | None = None,
        _stmt: str | None = None,
    ) -> int:
        """executemany version of orm_update_entries.

        Params like `set_cols_value` and `where_cols_value` need to be provided as iterables.
        NOTE that the execution will end and return when any of the input iterable exhausted.

        NOTE that `_extra_params` and `_extra_params_iter` will not be serialized. Caller needs to
            provide the serialized mappings ready for `executemany`.

        Args:
            set_cols (tuple[str, ...]): Cols to be updated.
            set_cols_value (Iterable[Mapping[str, Any]]): An iterable of values of to-be-updated cols.
            where_cols (tuple[str, ...] | None, optional): Cols to match. The WHERE stmt will be generated
                based on this param. Defaults to None.
            where_cols_value (Iterable[Mapping[str, Any]] | None, optional): An iterable of values of cols to match.
                Defaults to None.
            where_stmt (str | None, optional): Directly provide the WHERE stmt. If specified, both `where_cols` and
                `where_cols_value` will be ignored. Caller needs to feed the params with `_extra_params` or `_extra_params_iter`.
                Defaults to None.
            or_option (OR_OPTIONS | None, optional): specify the operation if UPDATE failed. Defaults to None.
            _extra_params (Mapping[str, Any] | None, optional): A fixed mapping to be injected for each execution.
                NOTE that this param is only allowed when at least one of `where_cols_value`, `set_cols_value` or `_extra_params_iter` is specified.
                Defaults to None.
            _extra_params_iter (Iterable[Mapping[str, Any]] | None, optional): An iterable of mappings to be injected for each execution. Defaults to None.
            _stmt (str | None, optional): Directly provide the UPDATE query, if specified, params except `_extra_params` and `_extra_params_iter`
                will be ignored. Defaults to None.

        Raises:
            ValueError: If `where_cols_value` and `where_cols` are not be both None or both specifed.
            ValueError: If `_stmt` is not used and `set_cols` and/or `set_cols_value` are not specified.
            ValueError: If `_extra_params` is specified without any other iterable params provided.
            sqlite3 DB error on execution failed.

        Returns:
            Affected rows count.
        """
        _table_spec = self.orm_table_spec

        params = None
        if not _stmt:
            # sanity check here
            if not (set_cols and set_cols_value):
                raise ValueError(
                    "if `_stmt` is not used, `set_cols` and `set_cols_value` are required"
                )
            if bool(where_cols_value) != bool(where_cols):
                raise ValueError(
                    "`where_cols_value` and `where_cols` MUST be both omitted or both specifed"
                )

            _stmt = _table_spec.table_update_stmt(
                update_target=self.orm_table_name,
                set_cols=set_cols,
                or_option=or_option,
                where_cols=where_cols,
                where_stmt=where_stmt,
            )

            params = _table_spec.table_serialize_mappings(set_cols_value)
            if where_cols_value and where_cols:
                params = _merge_iters(
                    _left=params,
                    _right=(
                        _table_spec.table_preprare_update_where_cols(_entry)
                        for _entry in _table_spec.table_serialize_mappings(
                            where_cols_value
                        )
                    ),
                )

        if _extra_params_iter:
            params = (
                _extra_params_iter
                if not params
                else _merge_iters(params, _extra_params_iter)
            )

        if not params:
            raise ValueError(
                "no param is provided! "
                "also only specified `_extra_params` without providing other iter params is not allowed"
            )
        if _extra_params:  # inject into param namespace for each execution
            params = (dict(**_param, **_extra_params) for _param in params)

        with self.orm_con as con:
            _cur = con.executemany(_stmt, params)
            return _cur.rowcount

    def orm_delete_entries(
        self,
        col_value_pairs: Mapping[str, Any] | None = None,
        *,
        _order_by: ColsDefinition | ColsDefinitionWithDirection | None = None,
        _limit: int | None = None,
        _stmt: str | None = None,
        **col_values: Any,
    ) -> int:
        """Delete entries from the table accordingly.

        Args:
            col_value_pairs(Mapping[str, Any] | None): provide col/value pairs by a Mapping, if provided,
                the pairs in this mapping will take prior than the one specified in <col_values>.
            _order_by (ColsDefinition | ColsDefinitionWithDirection | None, optional): Order the matching entries
                before executing the deletion, used together with <_limit>. Defaults to None.
            _limit (int | None, optional): Only delete <_limit> number of entries. Defaults to None.
            _stmt (str, optional): If provided, all params will be ignored and query statement will not
                be generated with the params, instead the provided <_stmt> will be used as query statement.
            **col_values: provide col/value pairs by kwargs. Col/value pairs in <col_values> have lower priority over
                the one specified by <_col_vlues_dict>.

        Returns:
            int: The num of entries deleted.
        """
        _table_spec = self.orm_table_spec
        if col_value_pairs:
            col_values.update(col_value_pairs)
        col_values = _table_spec.table_serialize_mapping(col_values)

        if not _stmt:
            _stmt = _table_spec.table_delete_stmt(
                delete_from=self.orm_table_name,
                limit=_limit,
                order_by=_order_by,
                returning_cols=None,
                where_cols=tuple(col_values),
            )

        with self._con as con:
            _cur = con.execute(_stmt, col_values)
            return _cur.rowcount

    @overload
    def orm_delete_entries_with_returning(
        self,
        col_value_pairs: Mapping[str, Any] | None = None,
        *,
        _order_by: ColsDefinition | ColsDefinitionWithDirection | None = None,
        _limit: int | None = None,
        _returning_cols: ColsDefinition | Literal["*"],
        _row_factory: Callable[[sqlite3.Cursor, Any], RT],
        _stmt: str | None = None,
        **col_values: Any,
    ) -> Generator[RT]: ...

    @overload
    def orm_delete_entries_with_returning(
        self,
        col_value_pairs: Mapping[str, Any] | None = None,
        *,
        _order_by: ColsDefinition | ColsDefinitionWithDirection | None = None,
        _limit: int | None = None,
        _returning_cols: ColsDefinition | Literal["*"],
        _row_factory: None = None,
        _stmt: str | None = None,
        **col_values: Any,
    ) -> Generator[TableSpecType | Any]: ...

    def orm_delete_entries_with_returning(
        self,
        col_value_pairs: Mapping[str, Any] | None = None,
        *,
        _order_by: ColsDefinition | ColsDefinitionWithDirection | None = None,
        _limit: int | None = None,
        _returning_cols: ColsDefinition | Literal["*"],
        _row_factory: RowFactoryType | None = None,
        _stmt: str | None = None,
        **col_values: Any,
    ) -> Generator[TableSpecType | Any]:
        """Delete entries from the table accordingly.

        NOTE that only sqlite3 version >= 3.35 supports returning statement.

        Args:
            col_value_pairs(Mapping[str, Any] | None): provide col/value pairs by a Mapping, if provided,
                the pairs in this mapping will take prior than the one specified in <col_values>.
            _order_by (ColsDefinition | ColsDefinitionWithDirection | None, optional): Order the matching entries
                before executing the deletion, used together with <_limit>. Defaults to None.
            _limit (int | None, optional): Only delete <_limit> number of entries. Defaults to None.
            _returning_cols (ColsDefinition | Literal["*"] ): Return the deleted entries on execution.
            _row_factory (RowFactoryType | None, optional): Set to use different row factory for this query.
                Defaults to None(do not change row_factory).
            _stmt (str, optional): If provided, all params will be ignored and query statement will not
                be generated with the params, instead the provided <_stmt> will be used as query statement.
            **col_values: provide col/value pairs by kwargs. Col/value pairs in <col_values> have lower priority over
                the one specified by <_col_vlues_dict>.

        Returns:
            Generator[TableSpecType | Any]: If <_returning_cols> is defined, returns a generator which can
                be used to yield the deleted entries from.
        """
        if col_value_pairs:
            col_values.update(col_value_pairs)

        if not _stmt:
            _stmt = self.orm_table_spec.table_delete_stmt(
                delete_from=self.orm_table_name,
                limit=_limit,
                order_by=_order_by,
                returning_cols=_returning_cols,
                where_cols=tuple(col_values),
            )

        def _gen():
            with self._con as con:
                _cur = con.execute(_stmt, col_values)
                if _row_factory is not None:
                    _cur.row_factory = _row_factory  # type: ignore
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
        if batch_size < 0:  # pragma: no cover
            raise ValueError("batch_size must be positive integer")

        _stmt = self.orm_table_spec.table_select_stmt(
            select_cols="rowid,*",
            select_from=self.orm_table_name,
            where_stmt="WHERE rowid > :not_before",
            limit=batch_size,
            order_by_stmt="ORDER BY rowid",
        )

        row_factory = self.orm_table_spec.table_from_tuple
        with self._con as con:
            con_exec = con.execute

            _not_before = 0
            while True:
                _cur = con_exec(_stmt, {"not_before": _not_before})
                _cur.row_factory = None  # let cursor returns raw row

                _row = None
                for _row in _cur:
                    yield row_factory(_row[1:])

                if _row is None:
                    return
                _not_before = _row[0]

    def orm_check_entry_exist(
        self, col_value_pairs: Mapping[str, Any] | None = None, **col_values: Any
    ) -> bool:
        """A quick method to check whether entry(entries) indicated by cols exists.

        This method uses COUNT function to count the selected entry.

        Args:
            col_value_pairs(Mapping[str, Any] | None): provide col/value pairs by a Mapping, if provided,
                the pairs in this mapping will take prior than the one specified in <col_values>.
            **cols: cols pair to locate the entry(entries).

        Returns:
            Returns True if at least one entry matches the input cols exists, otherwise False.
        """
        _table_spec = self.orm_table_spec
        if col_value_pairs:
            col_values.update(col_value_pairs)
        col_values = _table_spec.table_serialize_mapping(col_values)

        _stmt = _table_spec.table_select_stmt(
            select_from=self.orm_table_name,
            select_cols="*",
            function="count",
            where_cols=tuple(col_values),
        )
        with self._con as con:
            _cur = con.execute(_stmt, col_values)
            _cur.row_factory = None  # bypass con scope row_factory
            _res: tuple[int] = _cur.fetchone()
            return _res[0] > 0


ORMBaseType = TypeVar("ORMBaseType", bound=ORMBase)
