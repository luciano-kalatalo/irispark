from __future__ import annotations

import os
import re
import uuid
from typing import Any

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from .column import _quote
from .dataframe import IrisDataFrame

_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")


_ARROW_TYPE_TO_IRIS: dict[pa.DataType, str] = {
    pa.int8(): "INT",
    pa.int16(): "INT",
    pa.int32(): "INT",
    pa.int64(): "INT",
    pa.uint8(): "INT",
    pa.uint16(): "INT",
    pa.uint32(): "INT",
    pa.uint64(): "INT",
    pa.float16(): "DOUBLE",
    pa.float32(): "DOUBLE",
    pa.float64(): "DOUBLE",
    pa.bool_(): "INT",
    pa.date32(): "DATE",
    pa.date64(): "DATE",
    pa.timestamp("us"): "TIMESTAMP",
    pa.timestamp("ns"): "TIMESTAMP",
    pa.timestamp("ms"): "TIMESTAMP",
    pa.timestamp("s"): "TIMESTAMP",
    pa.decimal128(38, 0): "NUMERIC(38)",
}

_IRIS_TABLE_COUNTER = 0


def _arrow_to_iris_type(arrow_type: pa.DataType) -> str:
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP"
    if pa.types.is_decimal(arrow_type):
        return f"NUMERIC({arrow_type.precision},{arrow_type.scale})"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "VARCHAR(4000)"
    return _ARROW_TYPE_TO_IRIS.get(arrow_type, "VARCHAR(4000)")


def _serialize_value(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bytes):
        return _quote(v.decode("utf-8", errors="replace"))
    if isinstance(v, str):
        return _quote(v)
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    return _quote(str(v))


class Read:
    def __init__(self, session: Any) -> None:
        self._session = session

    def table(self, name: str) -> IrisDataFrame:
        return self._session.table(name)

    def parquet(
        self,
        path: str,
        foreign: bool = False,
        name: str | None = None,
        persistent: bool = False,
        options: dict[str, Any] | None = None,
    ) -> IrisDataFrame:
        if isinstance(path, str):
            src = path
        else:
            src = str(path)

        if foreign:
            df = self._session.iris.register_file_foreign_table(
                path=src,
                format="parquet",
                name=name,
                options=options,
                persistent=persistent,
            )
            df._append_lineage("source", f"read.parquet(foreign): {src}")
            return df

        table = pq.read_table(src)
        return self._table_to_iris_dataframe(table, src, "parquet")

    def csv(
        self,
        path: str,
        foreign: bool = False,
        name: str | None = None,
        persistent: bool = False,
        options: dict[str, Any] | None = None,
        server_path: str | None = None,
    ) -> IrisDataFrame:
        if isinstance(path, str):
            src = path
        else:
            src = str(path)

        if foreign:
            df = self._session.iris.register_file_foreign_table(
                path=src,
                format="csv",
                name=name,
                options=options,
                persistent=persistent,
                server_path=server_path,
            )
            df._append_lineage("source", f"read.csv(foreign): {src}")
            return df

        table = pcsv.read_csv(src)
        return self._table_to_iris_dataframe(table, src, "csv")

    def json(
        self,
        path: str,
        foreign: bool = False,
        name: str | None = None,
        persistent: bool = False,
        options: dict[str, Any] | None = None,
    ) -> IrisDataFrame:
        if isinstance(path, str):
            src = path
        else:
            src = str(path)

        if foreign:
            df = self._session.iris.register_file_foreign_table(
                path=src,
                format="json",
                name=name,
                options=options,
                persistent=persistent,
            )
            df._append_lineage("source", f"read.json(foreign): {src}")
            return df

        import pyarrow.json as pajson

        table = pajson.read_json(src)
        return self._table_to_iris_dataframe(table, src, "json")

    def jdbc(
        self,
        url: str,
        dbtable: str,
        user: str = "",
        password: str = "",
        driver: str = "",
        connection: str | None = None,
        classpath: str | None = None,
        properties: str | None = None,
    ) -> IrisDataFrame:
        """Read a remote JDBC table through an IRIS Foreign Table.

        Instead of copying all rows into a local temp table, this registers a
        transient IRIS Foreign Server and Foreign Table pointing at the remote
        table, then returns a DataFrame backed by that foreign table.

        Uses the official IRIS grammar: a named SQL Gateway connection
        (created on demand in %SYS, or reused via ``connection``) backs a
        ``FOREIGN DATA WRAPPER JDBC`` foreign server. ``classpath`` must point
        to the JDBC driver JAR(s) when creating a new connection.
        """
        if not _SAFE_IDENTIFIER_RE.match(dbtable):
            raise ValueError(
                f"Invalid table name '{dbtable}'. "
                f"Must match pattern: {_SAFE_IDENTIFIER_RE.pattern}"
            )

        df = self._session.iris.register_jdbc_foreign_table(
            url=url,
            dbtable=dbtable,
            user=user,
            password=password,
            driver=driver,
            connection=connection,
            classpath=classpath,
            properties=properties,
        )
        df._append_lineage("source", f"read.jdbc: {dbtable}")
        return df

    def load_data(self, path: str, table: str, format: str = "csv",
                  options: dict | None = None, **kwargs) -> IrisDataFrame:
        """Server-side LOAD DATA for bulk ingestion on IRIS.

        Uses IRIS native LOAD DATA command to ingest CSV or Parquet files
        that reside on the IRIS server filesystem.

        Args:
            path: File path on the IRIS server (e.g., '/irisapp/data/sales.csv')
            table: Target table name
            format: 'csv' or 'parquet'
            options: Dict of IRIS LOAD DATA parse options:
                - HEADER: True|False (default False)
                - DELIMITER: single char (default ',')
                - QUOTE: single char (default '"')
                - ESCAPE: single char (default '\\\\')
                - SKIPROWS: number of rows to skip (default 0)
                - MAXERRORS: max parse errors before abort (default 0)
            **kwargs: Additional IRIS-specific options

        Returns:
            IrisDataFrame with the loaded data

        Raises:
            ValueError: If path, table, or format are invalid
            ImportError: If format is not supported
        """
        # Validate parameters
        if not path or not isinstance(path, str):
            raise ValueError("path must be a non-empty string")

        if not table or not isinstance(table, str):
            raise ValueError("table must be a non-empty string")

        if format not in ("csv", "parquet"):
            raise ValueError(f"format must be 'csv' or 'parquet', got: {format}")

        # Validate table name
        from .sql_generator import _validate_identifier
        _validate_identifier("table", table)

        from .session import _quote_ident

        # Build LOAD DATA SQL
        fmt_upper = format.upper()
        sql_opts: list[str] = []

        # Default options
        header = options.get("HEADER", False) if options else False
        delimiter = options.get("DELIMITER", ",") if options else ","
        quote = options.get("QUOTE", '"') if options else '"'
        escape = options.get("ESCAPE", "\\\\") if options else "\\\\"
        skiprows = options.get("SKIPROWS", 0) if options else 0
        maxerrors = options.get("MAXERRORS", 0) if options else 0

        if header:
            sql_opts.append("PARSE(HEADER=1)")
        sql_opts.append(f"FIELDS TERMINATED BY {_quote_ident(delimiter)}")
        sql_opts.append(f"ENCLOSED BY {_quote_ident(quote)}")
        sql_opts.append(f"ESCAPED BY {_quote_ident(escape)}")
        if skiprows > 0:
            sql_opts.append(f"SKIP {skiprows} ROWS")
        if maxerrors > 0:
            sql_opts.append(f"MAXERROR {maxerrors}")

        sql = (
            f"LOAD DATA FROM FILE {_quote_ident(path)} "
            f"INTO TABLE {table} "
            f"FORMAT {fmt_upper} "
            f"{' '.join(sql_opts)}"
        )

        try:
            self._session.sql(sql)
        except Exception as e:
            # Fallback: if LOAD DATA fails, try read.csv/read.parquet
            # But first, check if the file path is absolute (required for server-side)
            if not os.path.isabs(path):
                raise ValueError(
                    f"LOAD DATA requires an absolute path on the IRIS server. "
                    f"Got relative path: {path}. "
                    f"Fallback failed: {e}"
                )
            raise ValueError(f"LOAD DATA failed: {e}")

        # Return the loaded data as a DataFrame
        # Use read.csv or read.parquet depending on format
        if format == "csv":
            return self.csv(path)
        else:
            return self.parquet(path)

    def _table_to_iris_dataframe(self, table: pa.Table, path: str, kind: str) -> IrisDataFrame:
        global _IRIS_TABLE_COUNTER
        _IRIS_TABLE_COUNTER += 1

        tbl = f"irispark_ingest_{uuid.uuid4().hex[:12]}"

        col_defs: list[str] = []
        col_names: list[str] = []
        safe_col_names: list[str] = []
        for field in table.schema:
            iris_type = _arrow_to_iris_type(field.type)
            safe_name = field.name.replace('"', '""')
            col_defs.append(f'"{safe_name}" {iris_type}')
            col_names.append(field.name)
            safe_col_names.append(safe_name)

        ddl = f"CREATE TABLE {tbl} ({', '.join(col_defs)})"
        try:
            self._session.sql(f"DROP TABLE {tbl}")
        except Exception:
            pass
        self._session.sql(ddl)

        rows = table.to_pylist()
        if rows:
            self._session._batch_insert(tbl, col_names, rows)

        self._session._tmp_tables.append(tbl)
        df = IrisDataFrame(session=self._session, table_name=tbl)
        df._schema = list(zip(col_names, [_arrow_to_iris_type(f.type) for f in table.schema]))
        df._base_schema = list(df._schema)
        df._append_lineage("source", f"read.{kind}: {path}")
        return df
