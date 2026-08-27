from __future__ import annotations

import re
import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Literal

import iris
import pandas as pd

from .catalog import Catalog
from .column import _quote
from .dataframe import IrisDataFrame
from .read import Read
from .session_iris_extensions import SessionIrisExtensions
from .sql_generator import SQLGenerator
from .udf import UDFRegistration

if TYPE_CHECKING:
    from .context import IrisSparkContext


# Global session tracker for decorator usage
_active_session: IrisParkSession | None = None

# Upper bound for a single multi-row INSERT ... SELECT ... UNION ALL statement.
# IRIS's SQL preparer has been observed to reject some large UNION ALL chains
# (SQLCODE -202) at content-dependent boundaries well below any documented
# limit; keeping statements small avoids that region entirely.
_INSERT_MAX_STATEMENT_BYTES = 8000


def get_active_session() -> IrisParkSession | None:
    """Get the currently active IrisParkSession for decorator usage."""
    return _active_session


def set_active_session(session: IrisParkSession | None) -> None:
    """Set the active session for decorator usage."""
    global _active_session
    _active_session = session


def _quote_ident(name: str) -> str:
    return f'"{name.replace(chr(34), chr(34) + chr(34))}"'


_REQUIRED_CONFIG_KEYS = ("host", "port", "namespace", "username", "password")
_OPTIONAL_CONFIG_KEYS = ("timeout", "sslconfig")
_TMP_TABLE_COUNTER = 0


def _infer_type(values: list) -> str:
    from decimal import Decimal
    seen = set()
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            seen.add("INT")
        elif isinstance(v, int):
            seen.add("INT")
        elif isinstance(v, (float, Decimal)):
            seen.add("DOUBLE")
        elif isinstance(v, str):
            try:
                int(v)
                seen.add("INT")
            except ValueError:
                try:
                    float(v)
                    seen.add("DOUBLE")
                except ValueError:
                    seen.add("VARCHAR(4000)")
        else:
            seen.add("VARCHAR(4000)")
    if not seen:
        return "VARCHAR(4000)"
    if "VARCHAR(4000)" in seen:
        return "VARCHAR(4000)"
    if "DOUBLE" in seen:
        return "DOUBLE"
    return "INT"


class SQLResult:
    def __init__(self, rows: list[Any], columns: list[str]) -> None:
        self.rows = rows
        self.columns = columns

    def __iter__(self) -> Iterator[Any]:
        yield self.rows
        yield self.columns

    def show(self) -> None:
        pdf = pd.DataFrame(self.rows, columns=self.columns)
        print(pdf)

    def collect(self) -> list[Any]:
        return self.rows

    def __repr__(self) -> str:
        return f"SQLResult(rows={len(self.rows)}, columns={self.columns})"


class IrisParkSession:
    def __init__(
        self,
        host: str,
        port: int,
        namespace: str,
        username: str,
        password: str,
        timeout: int | None = None,
        sslconfig: str | None = None,
    ) -> None:
        connect_kwargs: dict[str, Any] = {}
        if timeout is not None:
            connect_kwargs["timeout"] = timeout
        if sslconfig is not None:
            connect_kwargs["sslconfig"] = sslconfig
        self.conn = iris.connect(
            host,
            port,
            namespace,
            username,
            password,
            **connect_kwargs,
        )
        self._config: dict[str, Any] = {
            "host": host,
            "port": port,
            "namespace": namespace,
            "username": username,
            "password": password,
            "timeout": timeout,
            "sslconfig": sslconfig,
        }
        self._tmp_tables: list[str] = []
        self._temp_views: dict[str, str] = {}
        self._cache_registry: dict[str, str] = {}
        self._foreign_tables: list[str] = []
        self._foreign_table_servers: dict[str, str] = {}
        self._foreign_server_config: dict[str, dict[str, str]] = {}
        self._read: Read | None = None
        self._spark_context: IrisSparkContext | None = None
        self._udf_registration: UDFRegistration | None = None
        self._catalog: Catalog | None = None
        self._iris_extensions: SessionIrisExtensions | None = None
        self._observability: bool = False
        self._metrics: list[dict[str, Any]] = []
        from .sql.udf import install_all
        install_all(self)
        from .sql.udaf import install_all as install_udaf_all
        install_udaf_all(self)
        set_active_session(self)

    @property
    def sparkContext(self) -> IrisSparkContext:
        if self._spark_context is None:
            from .context import IrisSparkContext
            self._spark_context = IrisSparkContext(self)
        return self._spark_context

    @property
    def read(self) -> Read:
        if self._read is None:
            self._read = Read(self)
        return self._read

    @property
    def udf(self) -> UDFRegistration:
        if self._udf_registration is None:
            self._udf_registration = UDFRegistration(self)
        return self._udf_registration

    @property
    def catalog(self) -> Catalog:
        if self._catalog is None:
            self._catalog = Catalog(self)
        return self._catalog

    @property
    def iris(self) -> SessionIrisExtensions:
        if self._iris_extensions is None:
            self._iris_extensions = SessionIrisExtensions(self)
        return self._iris_extensions

    def config(self, key: str, value: Any | None = None) -> Any | None:
        """Get or set a session-level config value.

        ``key`` can be a dotted path (e.g. ``"irispark.observability"``).
        When ``value`` is ``None``, returns the current value.
        """
        if value is None:
            return self._config.get(key)
        self._config[key] = value
        if key == "irispark.observability":
            self._observability = bool(value)
        return value

    def __enter__(self) -> IrisParkSession:
        return self

    def __exit__(self, *args: Any) -> Literal[False]:
        self.close()
        return False

    def close(self) -> None:
        if self.conn is not None:
            try:
                cursor = self.conn.cursor()
                # Drop foreign tables and their servers first.
                for ftbl in self._foreign_tables:
                    try:
                        cursor.execute(f"DROP FOREIGN TABLE IF EXISTS {ftbl}")
                    except Exception:
                        pass
                for srv in set(self._foreign_table_servers.values()):
                    try:
                        cursor.execute(f"DROP FOREIGN SERVER IF EXISTS {srv}")
                    except Exception:
                        pass
                for tbl in self._tmp_tables:
                    try:
                        cursor.execute(f"DROP TABLE IF EXISTS {tbl}")
                    except Exception:
                        pass
                cursor.close()
            finally:
                self.conn.close()
                self.conn = None
                self._tmp_tables = []
                self._foreign_tables = []
                self._foreign_table_servers = {}
            self._temp_views = {}
        set_active_session(None)

    @classmethod
    def builder(cls) -> IrisParkSessionBuilder:
        return IrisParkSessionBuilder()

    def table(self, table_name: str) -> IrisDataFrame:
        df = IrisDataFrame(session=self, table_name=table_name)
        df._append_lineage("source", f"table: {table_name}")
        return df

    def _register_temp_view(self, name: str, df: IrisDataFrame) -> None:
        gen = SQLGenerator(df)
        self._temp_views[name] = gen._table_source()

    def _substitute_temp_views(self, query: str) -> str:
        for name, source in sorted(
            self._temp_views.items(), key=lambda x: -len(x[0])
        ):
            query = re.sub(
                rf"(?<!\.)\b{re.escape(name)}\b",
                source,
                query,
            )
        return query

    def sql(self, query: str, **kwargs: Any) -> SQLResult:
        import time
        start = time.perf_counter()
        for name, df in kwargs.items():
            gen = SQLGenerator(df)
            source = gen._table_source()
            query = query.replace("{" + name + "}", source)
        query = self._substitute_temp_views(query)
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            if cursor.description is not None:
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
            else:
                rows = []
                columns = []
            result = SQLResult(rows, columns)
        finally:
            elapsed = time.perf_counter() - start
            if getattr(self, "_observability", False):
                self._metrics.append({
                    "query": query[:200],
                    "elapsed_ms": round(elapsed * 1000, 2),
                    "rows_returned": len(rows) if cursor.description else 0,
                })
            cursor.close()
        return result

    def _batch_insert(self, table: str, columns: list[str], rows: list, batch_size: int = 100) -> None:
        """Bulk-insert rows using chunked multi-row INSERT statements.

        IRIS SQL has no multi-row ``VALUES (...), (...)`` clause; its supported
        multi-row form is ``INSERT INTO t (cols) SELECT ... UNION ALL SELECT ...``
        (IRIS SQL reference, "Multi-Row Inserts"). Chunks are capped by row
        count *and* by rendered statement size: IRIS's SQL preparer has been
        observed to reject some large UNION ALL chains with SQLCODE -202 at
        non-obvious content boundaries, so statements are kept well under the
        region where that triggers (see LESSONS_LEARNED). Single-row chunks use
        plain VALUES. A failed chunk raises immediately — no silent retry —
        because re-sending rows that may already be inserted would duplicate
        data.

        Handles both list[tuple] and list[dict] input formats.
        """
        if not rows:
            return
        # Convert rows to list of tuples if they are dicts
        if rows and isinstance(rows[0], dict):
            tuple_rows = [tuple(row[c] for c in columns) for row in rows]
        else:
            tuple_rows = rows

        if not tuple_rows:
            return

        # Quote column names with double quotes for IRIS
        quoted_cols = ", ".join(f'"{c.replace(chr(34), chr(34) + chr(34))}"' for c in columns)

        cursor = self.conn.cursor()
        try:
            prefix = f'INSERT INTO "{table}" ({quoted_cols}) '
            start = 0
            n = len(tuple_rows)
            while start < n:
                first_vals = ", ".join(_quote(v) for v in tuple_rows[start])
                if (
                    start + 1 == n
                    or len(prefix) + len(first_vals) > _INSERT_MAX_STATEMENT_BYTES
                ):
                    # Singleton chunk keeps the plain VALUES fast path.
                    stmt = f"{prefix}VALUES ({first_vals})"
                    end = start + 1
                else:
                    first_frag = f"SELECT {first_vals}"
                    chunk = [first_frag]
                    size = len(prefix) + len(first_frag)
                    j = start + 1
                    while j < n and len(chunk) < batch_size:
                        frag = f"SELECT {', '.join(_quote(v) for v in tuple_rows[j])}"
                        if size + 11 + len(frag) > _INSERT_MAX_STATEMENT_BYTES:
                            break
                        chunk.append(frag)
                        size += 11 + len(frag)
                        j += 1
                    stmt = f"{prefix}{' UNION ALL '.join(chunk)}"
                    end = j
                try:
                    cursor.execute(stmt)
                except Exception as exc:
                    raise RuntimeError(
                        f"bulk insert into {table!r} failed for rows "
                        f"[{start}..{end - 1}] of {n}; earlier "
                        f"chunks were committed and no retry was attempted to "
                        f"avoid duplicate inserts"
                    ) from exc
                start = end
        finally:
            cursor.close()

    def createDataFrame(self, data: Any, schema: Any = None) -> IrisDataFrame:
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            if schema is None:
                schema = list(data.columns)
            data = [tuple(row) for row in data.values]
        elif type(data).__module__.startswith("polars") and type(data).__name__ == "DataFrame":
            if schema is None:
                schema = data.columns
            data = data.rows()
        else:
            try:
                import dask.dataframe as dd
                if isinstance(data, dd.DataFrame):
                    if schema is None:
                        schema = list(data.columns)
                    data = data.compute()
                    data = [tuple(row) for row in data.values]
            except ImportError:
                pass

        global _TMP_TABLE_COUNTER
        _TMP_TABLE_COUNTER += 1
        uid = str(uuid.uuid4())[:8]
        tbl = f"irispark_tmp_{_TMP_TABLE_COUNTER}_{uid}"
        if data is None or len(data) == 0:
            cols = schema or []
            col_types = ["VARCHAR(4000)"] * len(cols)
            col_defs = ", ".join(f'{_quote_ident(c)} {t}' for c, t in zip(cols, col_types))
            try:
                self.sql(f'DROP TABLE "{tbl}"')
            except Exception:
                pass
            self.sql(f'CREATE TABLE "{tbl}" ({col_defs})')
            self._tmp_tables.append(tbl)
            df = IrisDataFrame(session=self, table_name=tbl)
            df._schema = list(zip(cols, col_types))
            df._append_lineage("source", "createDataFrame")
            return df
        # Normalize dict rows to tuples keyed by the first row's keys so that
        # schema inference (positional indexing) and inserts share one shape.
        if data and isinstance(data[0], dict):
            if schema is None:
                schema = list(data[0].keys())
            missing = {k for row in data for k in row} - set(schema)
            if missing:
                raise ValueError(
                    f"dict rows contain keys not in schema: {sorted(missing)}"
                )
            data = [tuple(row.get(c) for c in schema) for row in data]

        cols = schema or [f"col{i}" for i in range(len(data[0]))]
        sample_size = min(10, len(data))
        col_types = [
            _infer_type([row[i] for row in data[:sample_size]])
            for i in range(len(cols))
        ]
        col_defs = ", ".join(f'{_quote_ident(c)} {t}' for c, t in zip(cols, col_types))
        try:
            self.sql(f'DROP TABLE "{tbl}"')
        except Exception:
            pass
        self.sql(f'CREATE TABLE "{tbl}" ({col_defs})')
        self._batch_insert(tbl, cols, data)
        self._tmp_tables.append(tbl)
        df = IrisDataFrame(session=self, table_name=tbl)
        df._schema = list(zip(cols, col_types))
        df._append_lineage("source", "createDataFrame")
        return df


class IrisParkSessionBuilder:
    def __init__(self) -> None:
        self._config: dict[str, Any] = {}
        self._app_name: str | None = None

    def host(self, value: str) -> IrisParkSessionBuilder:
        self._config["host"] = value
        return self

    def port(self, value: int) -> IrisParkSessionBuilder:
        self._config["port"] = value
        return self

    def namespace(self, value: str) -> IrisParkSessionBuilder:
        self._config["namespace"] = value
        return self

    def username(self, value: str) -> IrisParkSessionBuilder:
        self._config["username"] = value
        return self

    def password(self, value: str) -> IrisParkSessionBuilder:
        self._config["password"] = value
        return self

    def timeout(self, value: int) -> IrisParkSessionBuilder:
        self._config["timeout"] = value
        return self

    def sslconfig(self, value: str) -> IrisParkSessionBuilder:
        self._config["sslconfig"] = value
        return self

    def appName(self, value: str) -> IrisParkSessionBuilder:
        self._app_name = value
        return self

    def config(self, key: str, value: Any) -> IrisParkSessionBuilder:
        _CONFIG_MAP = {
            "iris.host": "host",
            "iris.port": "port",
            "iris.namespace": "namespace",
            "iris.username": "username",
            "iris.password": "password",
        }
        mapped = _CONFIG_MAP.get(key)
        if mapped:
            self._config[mapped] = value
        else:
            self._config[key] = value
        return self

    def getOrCreate(self) -> IrisParkSession:
        missing = [k for k in _REQUIRED_CONFIG_KEYS if k not in self._config]
        if missing:
            raise ValueError(
                f"Incomplete configuration. Missing fields: {', '.join(missing)}"
            )
        existing = get_active_session()
        if existing is not None and existing.conn is not None:
            for key in _REQUIRED_CONFIG_KEYS + _OPTIONAL_CONFIG_KEYS:
                if self._config.get(key) != existing._config.get(key):
                    raise ValueError(
                        f"An active session exists with different {key} "
                        f"({existing._config.get(key)!r} != {self._config.get(key)!r}); "
                        "close it before creating a new one"
                    )
            return existing
        return IrisParkSession(**self._config)


class _BuilderDescriptor:
    def __get__(self, obj: Any, objtype: Any = None) -> IrisParkSessionBuilder:
        return IrisParkSessionBuilder()


class IrisSparkSession:
    builder = _BuilderDescriptor()
