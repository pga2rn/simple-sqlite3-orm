from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from typing import Any, Iterable, Literal, TypeVar

from pydantic import BaseModel
from pydantic.fields import FieldInfo
from typing_extensions import Self

from simple_sqlite3_orm._sqlite_spec import (
    INSERT_OR,
    ORDER_DIRECTION,
    SQLiteBuiltInFuncs,
)
from simple_sqlite3_orm._utils import (
    ConstrainRepr,
    TypeAffinityRepr,
    gen_sql_stmt,
    lru_cache,
)


class TableSpec(BaseModel):
    """Define table as pydantic model, with specific APIs."""

    @classmethod
    def _generate_where_stmt(
        cls,
        where_cols: tuple[str, ...] | None = None,
        where_stmt: str | None = None,
    ) -> str:
        if where_stmt:
            return where_stmt
        if where_cols:
            cls.table_check_cols(where_cols)
            _conditions = (f"{_col}=:{_col}" for _col in where_cols)
            _where_cols_stmt = " AND ".join(_conditions)
            return f"WHERE {_where_cols_stmt}"
        return ""

    @classmethod
    def _generate_order_by_stmt(
        cls,
        order_by: tuple[str | tuple[str, ORDER_DIRECTION], ...] | None = None,
        order_by_stmt: str | None = None,
    ) -> str:
        if order_by_stmt:
            return order_by_stmt
        if order_by:
            _order_by_stmts: list[str] = []
            for _item in order_by:
                if isinstance(_item, tuple):
                    _col, _direction = _item
                    cls.table_get_col_fieldinfo(_col)
                    _order_by_stmts.append(f"{_col} {_direction}")
                else:
                    _order_by_stmts.append(_item)
            return f"ORDER BY {','.join(_order_by_stmts)}"
        return ""

    @classmethod
    def _generate_returning_stmt(
        cls,
        returning_cols: tuple[str, ...] | Literal["*"] | None = None,
        returning_stmt: str | None = None,
    ) -> str:
        if returning_stmt:
            return returning_stmt
        if returning_cols == "*":
            return "RETURNING *"
        if isinstance(returning_cols, tuple):
            cls.table_check_cols(returning_cols)
            return f"RETURNING {','.join(returning_cols)}"
        return ""

    @classmethod
    def table_get_col_fieldinfo(cls, col: str) -> FieldInfo:
        """Check whether the <col> exists and returns the pydantic FieldInfo.

        Raises:
            ValueError on non-existed col.
        """
        if metadata := cls.model_fields.get(col):
            return metadata
        raise ValueError(f"{col} is not defined in {cls=}")

    @classmethod
    @lru_cache
    def table_check_cols(cls, cols: tuple[str, ...]) -> None:
        """Ensure all cols in <cols> existed in the table definition.

        Raises:
            ValueError if any of col doesn't exist in the table.
        """
        for col in cols:
            if col not in cls.model_fields:
                raise ValueError(f"{col} is not defined in {cls=}")

    @classmethod
    @lru_cache
    def table_dump_column(cls, column_name: str) -> str:
        """Dump the column statement for table creation.

        Raises:
            ValueError on col doesn't exist or invalid col definition.
        """
        datatype_name, constrain = "", ""
        column_meta = cls.table_get_col_fieldinfo(column_name)

        for metadata in column_meta.metadata:
            if isinstance(metadata, TypeAffinityRepr):
                datatype_name = metadata
            elif isinstance(metadata, ConstrainRepr):
                constrain = metadata
        if not datatype_name:
            datatype_name = TypeAffinityRepr(column_meta.annotation)

        res = f"{column_name} {datatype_name} {constrain}".strip()
        return res

    @classmethod
    @lru_cache
    def table_create_stmt(
        cls,
        table_name: str,
        *,
        if_not_exists: bool = False,
        strict: bool = False,
        temporary: bool = False,
        without_rowid: bool = False,
    ) -> str:
        """Get create table statement with this table spec class.

        Check https://www.sqlite.org/lang_createtable.html for more details.
        """

        cols_spec = ",".join(
            cls.table_dump_column(col_name) for col_name in cls.model_fields
        )
        table_options: list[str] = []
        if without_rowid:
            table_options.append("WITHOUT ROWID")
        if strict:
            table_options.append("STRICT")

        res = gen_sql_stmt(
            "CREATE",
            f"{'TEMPORARY' if temporary else ''}",
            "TABLE",
            f"{'IF NOT EXISTS' if if_not_exists else ''}",
            f"{table_name} ({cols_spec})",
            f"{','.join(table_options)}",
        )
        return res

    @classmethod
    @lru_cache
    def table_create_index_stmt(
        cls,
        *,
        table_name: str,
        index_name: str,
        index_cols: tuple[str | tuple[str, ORDER_DIRECTION], ...],
        if_not_exists: bool = False,
        unique: bool = False,
    ) -> str:
        """Get index create statement with this table spec class.

        Raises:
            ValueError on <index_cols> not specified, or invalid <index_cols>.

        Check https://www.sqlite.org/lang_createindex.html for more details.
        """
        if not index_cols:
            raise ValueError("at least one col should be specified for an index")

        indexed_cols: list[str] = []
        for _input in index_cols:
            if isinstance(_input, tuple):
                _col, _order = _input
                cls.table_get_col_fieldinfo(_col)
                indexed_cols.append(f"{_col} {_order}")
            else:
                _col = _input
                cls.table_get_col_fieldinfo(_col)
                indexed_cols.append(_col)
        indexed_columns_stmt = f"({','.join(indexed_cols)})"

        res = gen_sql_stmt(
            "CREATE",
            f"{'UNIQUE' if unique else ''}",
            "INDEX",
            f"{'IF NOT EXISTS' if if_not_exists else ''}",
            f"{index_name}",
            f"ON {table_name} {indexed_columns_stmt}",
        )
        return res

    @classmethod
    def table_row_factory(
        cls,
        _cursor: sqlite3.Cursor,
        _row: tuple[Any, ...] | Any,
        *,
        validation: bool = True,
    ) -> Self | tuple[Any, ...]:
        """A general row_factory implement for used in sqlite3 connection.

        When the input <_row> is not a row but something like function output,
            this method will return the raw input tuple as it.

        Args:
            validation (bool): whether enable pydantic validation when importing row. Default to True.

        Raises:
            Any exception raised from pydantic model_validate or model_construct.

        Returns:
            An instance of <TableSpec>, or the raw tuple if the input row has different schema.

        Also see https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.description
            for more details.
        """
        _fields = [col[0] for col in _cursor.description]

        # when we realize that the input is not a row, but something like function call's output.
        if not all(col in cls.model_fields for col in _fields):
            return _row

        if validation:
            return cls.model_validate(dict(zip(_fields, _row)))
        return cls.model_construct(**dict(zip(_fields, _row)))

    @classmethod
    def table_row_factory2(
        cls,
        _cursor: sqlite3.Cursor,
        _row: tuple[Any, ...] | Any,
        *,
        validation: bool = True,
    ) -> Self:
        """Another version of row_factory implementation for specific use case.

        Unlike table_row_factory that checks the cols definition and ensure all cols
            are valid and presented in table def, table_row_factory2 just picks the valid
            cols from the input raw table row, and ignore unknown col/value pairs.

        Args:
            validation (bool): whether enable pydantic validation when importing row. Default to True.

        Raises:
            Any exception raised from pydantic model_validate or model_construct.

        Returns:
            An instance of <TableSpec>.
        """
        _fields = [col[0] for col in _cursor.description]
        _to_be_processed = {
            k: v for k, v in zip(_fields, _row) if k in cls.model_fields
        }
        if validation:
            return cls.model_validate(_to_be_processed)
        return cls.model_construct(**_to_be_processed)

    @classmethod
    def table_from_tuple(
        cls, _row: Iterable[Any], *, with_validation: bool = True, **kwargs
    ) -> Self:
        """A raw row_factory that converts the input _row to TableSpec instance.

        NOTE that this method expects the input row match the table row spec EXACTLY.

        Args:
            _row (tuple[Any, ...]): the raw table row as tuple.
            with_validation (bool): if set to False, will use pydantic model_construct to directly
                construct instance without validation. Default to True.
            **kwargs: extra kwargs passed to pydantic model_validate API. Note that only when
                with_validation is True, the kwargs will be used.

        Returns:
            An instance of self.
        """
        if with_validation:
            return cls.model_validate(dict(zip(cls.model_fields, _row)), **kwargs)
        return cls.model_construct(**dict(zip(cls.model_fields, _row)))

    @classmethod
    def table_from_dict(
        cls, _map: Mapping[str, Any], *, with_validation: bool = True, **kwargs
    ) -> Self:
        """A raw row_factory that converts the input mapping to TableSpec instance.

        Args:
            _map (Mapping[str, Any]): the raw table row as a dict.
            with_validation (bool, optional): if set to False, will use pydantic model_construct to directly
                construct instance without validation. Default to True.
            **kwargs: extra kwargs passed to pydantic model_validate API. Note that only when
                with_validation is True, the kwargs will be used.

        Returns:
            An instance of self.
        """
        if with_validation:
            return cls.model_validate(_map, **kwargs)
        return cls.model_construct(**_map)

    @classmethod
    @lru_cache
    def table_insert_stmt(
        cls,
        *,
        insert_into: str,
        insert_cols: tuple[str, ...] | None = None,
        insert_default: bool = False,
        or_option: INSERT_OR | None = None,
        returning_cols: tuple[str, ...] | Literal["*"] | None = None,
        returning_stmt: str | None = None,
    ) -> str:
        """Get sql for inserting row(s) into <table_name>.

        Check https://www.sqlite.org/lang_insert.html for more details.

        Args:
            insert_into (str): The name of table insert into.
            insert_cols (tuple[str, ...] | None, optional): The cols to be assigned for entry to be inserted.
                Defaults to None, means we will assign all cols of the row.
            insert_default (bool, optional): No values will be assigned, all cols will be assigned with
                default value, this precedes the <insert_cols> param. Defaults to False.
            or_option (INSERT_OR | None, optional): The fallback operation if insert failed. Defaults to None.
            returning_cols (tuple[str, ...] | Literal["*"] | None): Which cols are included in the returned entries.
                Defaults to None.
            returning_stmt (str | None, optional): The full returning statement string, this
                precedes the <returning_cols> param. Defaults to None.

        Returns:
            str: The generated insert statement.
        """
        if or_option:
            gen_or_option_stmt = f"OR {or_option.upper()}"
        else:
            gen_or_option_stmt = ""

        gen_insert_stmt = f"INSERT {gen_or_option_stmt} INTO {insert_into}"

        if insert_default:
            gen_insert_value_stmt = "DEFAULT VALUES"
        elif insert_cols:
            cls.table_check_cols(insert_cols)

            _cols_named_placeholder = (f":{_col}" for _col in insert_cols)
            gen_insert_value_stmt = f"VALUES ({','.join(_cols_named_placeholder)})"
        else:
            _cols_named_placeholder = (f":{_col}" for _col in cls.model_fields)
            gen_insert_value_stmt = f"VALUES ({','.join(_cols_named_placeholder)}) "

        gen_returning_stmt = cls._generate_returning_stmt(
            returning_cols, returning_stmt
        )

        res = gen_sql_stmt(
            gen_insert_stmt,
            gen_insert_value_stmt,
            gen_returning_stmt,
        )
        return res

    @classmethod
    @lru_cache
    def table_select_all_stmt(
        cls,
        *,
        select_from: str,
        batch_idx: int | None = None,
        batch_size: int | None = None,
        order_by: tuple[str | tuple[str, ORDER_DIRECTION], ...] | None = None,
        order_by_stmt: str | None = None,
        distinct: bool = False,
    ) -> str:
        """Select all entries, optionally with pagination LIMIT and OFFSET.

        Check https://www.sqlite.org/lang_select.html for more details.

        Args:
            select_from (str): The table name for the generated statement.
            batch_idx (int | None, optional): If specified, the batch index of current page. Defaults to None.
            batch_size (int | None, optional): If specified, the batch size of each page. Defaults to None.
            order_by (tuple[str | tuple[str, ORDER_DIRECTION], ...] | None, optional):
                A list of cols for ordering result. Defaults to None.
            order_by_stmt (str | None, optional): The order_by statement string, this
                precedes the <order_by> param if set. Defaults to None.
            distinct (bool, optional): Whether filters the duplicated entries. Defaults to False.

        Raises:
            ValueError: If batch_idx or bach_size are not both None or not None, or the values are invalid.

        Returns:
            str: The generated select statement.
        """
        if (batch_idx is not None and batch_size is None) or (
            batch_idx is None and batch_size is not None
        ):
            raise ValueError(
                "batch_idx and batch_size must be None or not None together"
            )

        gen_select_stmt = f"SELECT {'DISTINCT ' if distinct else ''}"
        gen_select_from_stmt = f"* from {select_from}"
        gen_order_by_stmt = cls._generate_order_by_stmt(order_by, order_by_stmt)

        gen_pagination = ""
        if batch_idx is not None and batch_size is not None:
            if batch_size <= 0 or batch_idx < 0:
                raise ValueError(f"invalid {batch_idx=} or {batch_size}")
            gen_pagination = f"LIMIT {batch_size} OFFSET {batch_idx * batch_size}"

        res = gen_sql_stmt(
            gen_select_stmt,
            gen_select_from_stmt,
            gen_order_by_stmt,
            gen_pagination,
        )
        return res

    @classmethod
    @lru_cache
    def table_select_stmt(
        cls,
        *,
        select_from: str,
        select_cols: tuple[str, ...] | Literal["*"] | str = "*",
        function: SQLiteBuiltInFuncs | None = None,
        where_stmt: str | None = None,
        where_cols: tuple[str, ...] | None = None,
        group_by: tuple[str, ...] | None = None,
        order_by: tuple[str | tuple[str, ORDER_DIRECTION], ...] | None = None,
        order_by_stmt: str | None = None,
        limit: int | None = None,
        distinct: bool = False,
    ) -> str:
        """Get sql for getting row(s) from <table_name>, optionally with
            where condition specified by <col_values>.

        Check https://www.sqlite.org/lang_select.html for more details.

        Args:
            select_from (str): The table name for the generated statement.
            select_cols (tuple[str, ...] | Literal["*"] | str, optional): A list of cols included in the result row. Defaults to "*".
            function (SQLiteBuiltInFuncs | None, optional): The sqlite3 function used in the selection. Defaults to None.
            where_cols (tuple[str, ...] | None, optional): A list of cols to be compared in where
                statement. Defaults to None.
            where_stmt (str | None, optional): The full where statement string, this
                precedes the <where_cols> param if set. Defaults to None.
            group_by (tuple[str, ...] | None, optional): A list of cols for group_by statement. Defaults to None.
            order_by (Iterable[str  |  tuple[str, ORDER_DIRECTION], ...] | None, optional):
                A list of cols for ordering result. Defaults to None.
            order_by_stmt (str | None, optional): The order_by statement string, this
                precedes the <order_by> param if set. Defaults to None.
            limit (int | None, optional): Limit the number of result entries. Defaults to None.
            distinct (bool, optional): Whether filters the duplicated entries. Defaults to False.

        Returns:
            str: The generated select statement.
        """
        if isinstance(select_cols, tuple):
            cls.table_check_cols(select_cols)
            select_target = ",".join(select_cols)
        else:
            select_target = select_cols

        if function:
            select_target = f"{function}({select_target})"

        gen_select_stmt = f"SELECT {'DISTINCT ' if distinct else ''}"
        gen_select_from_stmt = f"{select_target} FROM {select_from}"
        gen_where_stmt = cls._generate_where_stmt(where_cols, where_stmt)
        gen_group_by_stmt = f"GROUP BY {','.join(group_by)}" if group_by else ""
        gen_order_by_stmt = cls._generate_order_by_stmt(order_by, order_by_stmt)
        gen_limit_stmt = f"LIMIT {limit}" if limit is not None else ""

        res = gen_sql_stmt(
            gen_select_stmt,
            gen_select_from_stmt,
            gen_where_stmt,
            gen_group_by_stmt,
            gen_order_by_stmt,
            gen_limit_stmt,
        )
        return res

    @classmethod
    @lru_cache
    def table_delete_stmt(
        cls,
        *,
        delete_from: str,
        where_cols: tuple[str, ...] | None = None,
        where_stmt: str | None = None,
        order_by: tuple[str | tuple[str, ORDER_DIRECTION], ...] | None = None,
        order_by_stmt: str | None = None,
        limit: int | str | None = None,
        returning_cols: tuple[str, ...] | Literal["*"] | None = None,
        returning_stmt: str | None = None,
    ) -> str:
        """Get sql for deleting row(s) from <table_name> with specifying col value(s).

        Check https://www.sqlite.org/lang_delete.html for more details.

        NOTE(20240311): DELETE operation without any condition(no WHERE statement) in
            WITHOUT_ROWID table will result in rowcount=0, see
            https://sqlite.org/forum/forumpost/07dedbf9a1 for more details.
            For python, python version < 3.10 will be affected by this bug.
            A quick workaround is to add any condition in where statement, even a dummy
            "WHERE 1=1" can resolve the above bug.
            I will not add this hack here, and user can add this hack according to their needs.

        NOTE: <order_by> and <limit> support are only enabled when runtime sqlite3 lib is compiled with
            SQLITE_ENABLE_UPDATE_DELETE_LIMIT flag.

        Args:
            delete_from (str): The table name for the generated statement.
            limit (int | str | None, optional): The value for limit expr. Defaults to None.
            order_by (Iterable[str  |  tuple[str, ORDER_DIRECTION]] | None, optional):
                A list of cols for ordering result. Defaults to None.
            order_by_stmt (str | None, optional): The order_by statement string, this
                precedes the <order_by> param if set. Defaults to None.
            where_cols (tuple[str, ...] | None, optional): A list of cols to be compared in where
                statement. Defaults to None.
            where_stmt (str | None, optional): The full where statement string, this
                precedes the <where_cols> param if set. Defaults to None.
            returning_cols (tuple[str, ...] | Literal["*"] | None): Which cols are included in the returned entries.
                Defaults to None.
            returning_stmt (str | None, optional): The full returning statement string, this
                precedes the <returning_cols> param. Defaults to None.

        Returns:
            str: The generated delete statement.
        """
        gen_delete_from_stmt = f"DELETE FROM {delete_from}"
        gen_where_stmt = cls._generate_where_stmt(where_cols, where_stmt)
        gen_returning_stmt = cls._generate_returning_stmt(
            returning_cols, returning_stmt
        )
        gen_order_by_stmt = cls._generate_order_by_stmt(order_by, order_by_stmt)
        gen_limit_stmt = f"LIMIT {limit}" if limit is not None else ""

        res = gen_sql_stmt(
            gen_delete_from_stmt,
            gen_where_stmt,
            gen_returning_stmt,
            gen_order_by_stmt,
            gen_limit_stmt,
        )
        return res

    def table_dump_asdict(self, *cols: str, **kwargs) -> dict[str, Any]:
        """Dump self as a dict, containing all cols or specified cols.

        Under the hook this method calls pydantic model_dump on self.
        The dumped dict can be used to directly insert into the table.

        Args:
            *cols: which cols to export, if not specified, export all cols.
            **kwargs: any other kwargs that passed to pydantic model_dump method.
                Note that the include kwarg is used to specific which cols to dump.

        Raises:
            ValueError if failed to serialize the model, wrapping underlying
                pydantic serialization error.

        Returns:
            A dict of dumped col values from this row.
        """
        try:
            _included_cols = set(cols) if cols else None
            return self.model_dump(include=_included_cols, **kwargs)
        except Exception as e:
            raise ValueError(f"failed to dump as dict: {e!r}") from e

    def table_dump_astuple(self, *cols: str, **kwargs) -> tuple[Any, ...]:
        """Dump self's values as a tuple, containing all cols or specified cols.

        This method is basically the same as table_dump_asdict, but instead return a
            tuple of the dumped values.

        Returns:
            A tuple of dumped col values.
        """
        try:
            _included_cols = set(cols) if cols else None
            return tuple(self.model_dump(include=_included_cols, **kwargs).values())
        except Exception as e:
            raise ValueError(f"failed to dump as tuple: {e!r}") from e


TableSpecType = TypeVar("TableSpecType", bound=TableSpec)
