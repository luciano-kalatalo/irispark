from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from .sql_generator import _validate_identifier

if TYPE_CHECKING:
    from .dataframe import IrisDataFrame


def _quote_table(name: str) -> str:
    return ".".join(f'"{p}"' for p in name.split("."))


class DataFrameWriter:
    def __init__(self, dataframe: IrisDataFrame) -> None:
        self._df = dataframe
        self._mode: str = "error"
        self._storage_type: str | None = None

    def mode(self, saveMode: str) -> DataFrameWriter:
        key = saveMode.lower()
        if key == "errorifexists":
            key = "error"
        if key not in ("append", "overwrite", "error", "ignore"):
            raise ValueError(
                f"Modo invalido: {saveMode!r}. "
                f"Valores aceitos: append, overwrite, error, errorifexists, ignore"
            )
        self._mode = key
        return self

    def storageType(self, storageType: str) -> DataFrameWriter:
        """Set the storage type for the target table.

        Args:
            storageType: Either 'row' or 'columnar' (case-insensitive).

        Returns:
            DataFrameWriter: Self for method chaining.
        """
        key = storageType.lower()
        if key not in ("row", "columnar"):
            raise ValueError(
                f"Invalid storage type: {storageType!r}. Use 'row' or 'columnar'."
            )
        self._storage_type = key.upper()
        return self

    def _table_exists(self, table_name: str) -> bool:
        try:
            rows, _ = self._df.session.sql(
                f"SELECT COUNT(*) FROM {_quote_table(table_name)}"
            )
            return True
        except Exception:
            return False

    def _table_has_rows(self, table_name: str) -> bool:
        try:
            rows, _ = self._df.session.sql(
                f"SELECT COUNT(*) FROM {_quote_table(table_name)}"
            )
            return rows[0][0] > 0
        except Exception:
            return False

    def saveAsTable(self, table_name: str) -> None:
        _validate_identifier("table_name", table_name)
        sql = self._df.to_sql()
        exists = self._table_exists(table_name)

        if self._mode == "error" and exists:
            raise ValueError(
                f"Table {table_name!r} already exists. "
                f"Use mode('overwrite') or mode('append') to write to it."
            )

        if self._mode == "ignore" and exists:
            return

        if self._mode == "append" and exists:
            self._df.session.sql(
                f"INSERT INTO {_quote_table(table_name)} {sql}"
            )
            return

        try:
            self._df.session.sql(f"DROP TABLE {_quote_table(table_name)}")
        except Exception:
            pass
        storage_clause = f" WITH STORAGETYPE = {self._storage_type}" if self._storage_type else ""
        ddl = f"CREATE TABLE {_quote_table(table_name)} AS {sql}{storage_clause}"
        self._df.session.sql(ddl)

    def insertInto(self, table_name: str) -> None:
        _validate_identifier("table_name", table_name)
        sql = self._df.to_sql()
        has_rows = self._table_has_rows(table_name)

        if self._mode == "error" and has_rows:
            raise ValueError(
                f"Table {table_name!r} already has data. "
                f"Use mode('overwrite') or mode('append') to write to it."
            )

        if self._mode == "ignore" and has_rows:
            return

        if self._mode == "overwrite":
            try:
                self._df.session.sql(f"DELETE FROM {_quote_table(table_name)}")
            except Exception:
                pass

        self._df.session.sql(
            f"INSERT INTO {_quote_table(table_name)} SELECT * FROM ({sql}) AS _src"
        )

    def csv(self, path: str) -> None:
        self._df.to_pandas().to_csv(path, index=False)

    def parquet(self, path: str) -> None:
        self._df.to_pandas().to_parquet(path, index=False)

    def jdbc(
        self,
        url: str,
        dbtable: str,
        user: str = "",
        password: str = "",
        driver: str = "",
        mode: str = "append",
        name: str | None = None,
    ) -> IrisDataFrame:
        """Write this DataFrame to a remote JDBC table via an IRIS Foreign Table.

        IRIS evaluates the DataFrame's generated SQL inside the database and pushes
        the resulting rows through the foreign server to the remote table. No rows
        are materialized in Python.

        Args:
            url: JDBC connection URL for the foreign server.
            dbtable: Remote target table name (may include schema).
            user: Optional JDBC user.
            password: Optional JDBC password.
            driver: Optional JDBC driver class name.
            mode: ``append`` (default) or ``overwrite``.
            name: Optional IRIS foreign table name. Generated if omitted.

        Returns:
            IrisDataFrame backed by the foreign table used for the write.
        """
        from .sql_generator import _validate_identifier

        _validate_identifier("dbtable", dbtable)

        if name is None:
            name = f"irispark_ftbl_{uuid.uuid4().hex[:12]}"
        _validate_identifier("name", name)

        if mode not in ("append", "overwrite"):
            raise ValueError(
                f"Invalid jdbc mode: {mode!r}. Use 'append' or 'overwrite'."
            )

        source_sql = self._df.to_sql()

        # Register a transient foreign table over the remote JDBC target (the
        # official grammar: named connection + SERVER ... TABLE 'target'), then
        # push rows through it.
        ft = self._df.session.iris.register_jdbc_foreign_table(
            url=url,
            dbtable=dbtable,
            user=user,
            password=password,
            driver=driver,
            name=name,
            persistent=False,
        )
        return self._execute_write(ft, source_sql, mode)

    def _execute_write(
        self, ft: IrisDataFrame, source_sql: str, mode: str
    ) -> IrisDataFrame:
        from .session import _quote_ident

        ft_name = ft.table_name
        if mode == "overwrite":
            try:
                self._df.session.sql(f"DELETE FROM {_quote_ident(ft_name)}")
            except Exception:
                pass
        self._df.session.sql(
            f"INSERT INTO {_quote_ident(ft_name)} {source_sql}"
        )
        return ft

    def saveAsForeignTable(
        self,
        name: str,
        server_name: str,
        remote_table: str,
        *,
        mode: str = "overwrite",
        persistent: bool = True,
    ) -> IrisDataFrame:
        """Publish this DataFrame as a named foreign table on an existing server.

        Args:
            name: IRIS foreign table name.
            server_name: Existing foreign server name in this session.
            remote_table: Remote target table name.
            mode: ``overwrite`` (default) or ``append``.
            persistent: If True, the foreign table survives ``session.close()``.

        Returns:
            IrisDataFrame backed by the foreign table.
        """
        source_sql = self._df.to_sql()
        ft = self._df.session.iris.create_foreign_table_from_query(
            name=name,
            server_name=server_name,
            remote_table=remote_table,
            select_sql=source_sql,
            persistent=persistent,
        )
        return self._execute_write(ft, source_sql, mode)
