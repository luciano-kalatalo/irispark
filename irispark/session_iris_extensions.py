from __future__ import annotations

import json
import os
import uuid
import warnings
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .dataframe import IrisDataFrame
    from .session import IrisParkSession


# IRIS 2026.2 supports exactly two foreign data wrappers (RSQL_createserver):
#   CSV  — local .csv files; the folder path goes in the required HOST clause.
#   JDBC — named JDBC connections (see register_jdbc_foreign_table).
# Parquet/JSON have no standard wrapper here; use the client-side readers or
# the iris-parquet project's custom FDW.
_FILE_FORMAT_TO_WRAPPER = {
    "csv": "CSV",
}

# Options translated into the CREATE FOREIGN TABLE USING {"from": {"file": ...}}
# JSON tree (same semantics as LOAD DATA; RSQL_createforeigntable).
_USING_FILE_OPTIONS = {
    "header",
    "columnseparator",
    "escapechar",
}


class SessionIrisExtensions:
    """IRIS-specific extensions tied to a session.

    Provides lifecycle management for IRIS Foreign Tables and related
    server-side objects that have no direct PySpark equivalent.

    Usage:
        session.iris.register_jdbc_foreign_table(...)
        session.iris.register_file_foreign_table(...)
        session.iris.foreign_tables()
        session.iris.drop_foreign_table("ft_name")
    """

    def __init__(self, session: IrisParkSession) -> None:
        self._session = session
        self._ensure_state()

    def _ensure_state(self) -> None:
        """Lazily initialize foreign-table bookkeeping on the session."""
        if not hasattr(self._session, "_foreign_tables"):
            self._session._foreign_tables = []
        if not hasattr(self._session, "_foreign_table_servers"):
            self._session._foreign_table_servers = {}
        if not hasattr(self._session, "_foreign_server_config"):
            self._session._foreign_server_config = {}

    def foreign_tables(self) -> list[str]:
        """Return the list of foreign tables registered in this session."""
        return list(getattr(self._session, "_foreign_tables", []))

    def register_jdbc_foreign_table(
        self,
        url: str,
        dbtable: str,
        user: str = "",
        password: str = "",
        driver: str = "",
        name: str | None = None,
        options: dict[str, Any] | None = None,
        persistent: bool = False,
        connection: str | None = None,
        classpath: str | None = None,
        properties: str | None = None,
    ) -> IrisDataFrame:
        """Register a JDBC-backed IRIS Foreign Table and return a DataFrame.

        Uses the official IRIS grammar (RSQL_createserver / RSQL_createforeigntable):
        a *named* JDBC connection is registered in the instance's SQL Gateway
        (``%Library.sys_SQLConnection`` in %SYS), then the server is created with
        ``FOREIGN DATA WRAPPER JDBC CONNECTION '<name>'`` and the table with
        ``SERVER <srv> TABLE '<dbtable>'``. IRIS owns the remote schema, so no
        client-side introspection is needed.

        Args:
            url: JDBC connection URL (e.g. ``jdbc:IRIS://host:1972/namespace``).
            dbtable: Remote table or view name (may include schema, e.g. 'schema.table').
            user: JDBC user name used to build the named connection.
            password: JDBC password used to build the named connection.
            driver: JDBC driver class (e.g. ``com.intersystems.jdbc.IRISDriver``).
            name: Optional explicit IRIS foreign table name. If omitted, a
                unique name is generated.
            options: Ignored; kept for API compatibility.
            persistent: If True, the foreign table is NOT added to the session's
                transient cleanup list and survives ``session.close()``.
            connection: Reuse an existing named SQL Gateway connection instead of
                creating one. When given, ``url``/``user``/``password``/``driver``
                are ignored.
            classpath: JAR(s) (comma-separated) the server needs to load the JDBC
                driver (e.g. ``/usr/irissys/dev/java/lib/iris-jdbc.jar``). Only
                used when creating a new named connection.
            properties: Optional vendor JDBC connection properties
                (``k=v; k2=v2``). Only used when creating a new named connection.

        Returns:
            IrisDataFrame backed by the newly created foreign table.
        """
        from .dataframe import IrisDataFrame
        from .session import _quote, _quote_ident
        from .sql_generator import _validate_identifier

        _validate_identifier("dbtable", dbtable)

        if name is None:
            name = f"irispark_ftbl_{uuid.uuid4().hex[:12]}"
        _validate_identifier("name", name)

        # Ensure a named SQL Gateway connection exists (create when absent).
        if connection:
            conn_name = connection
        else:
            conn_name = self._ensure_jdbc_connection(
                url=url,
                user=user,
                password=password,
                driver=driver,
                classpath=classpath,
                properties=properties,
            )

        server_name = f"irispark_fsrv_{uuid.uuid4().hex[:12]}"
        drop_table_sql = f"DROP FOREIGN TABLE IF EXISTS {_quote_ident(name)}"
        create_server_sql = (
            f"CREATE FOREIGN SERVER {_quote_ident(server_name)} "
            f"FOREIGN DATA WRAPPER JDBC CONNECTION {_quote(conn_name)}"
        )
        create_table_sql = (
            f"CREATE FOREIGN TABLE {_quote_ident(name)} "
            f"SERVER {_quote_ident(server_name)} TABLE {_quote(dbtable)}"
        )

        try:
            self._session.sql(drop_table_sql)
        except Exception:
            pass

        self._session.sql(create_server_sql)
        self._session.sql(create_table_sql)

        # Discover the projected schema via a LIMIT 0 probe against the foreign
        # table, so joins/selects know the remote column names and types.
        col_schema: list[tuple[str, str]] = []
        try:
            rows, columns = self._session.sql(
                f"SELECT * FROM {_quote_ident(name)} LIMIT 0"
            )
            col_schema = [(str(c), "VARCHAR(4000)") for c in columns]
        except Exception:
            col_schema = []

        self._session._foreign_table_servers[name] = server_name
        self._session._foreign_server_config[server_name] = {
            "connection": conn_name,
            "dbtable": dbtable,
        }
        if not persistent:
            self._session._foreign_tables.append(name)

        df = IrisDataFrame(session=self._session, table_name=name)
        df._schema = col_schema
        return df

    def _ensure_jdbc_connection(
        self,
        url: str,
        user: str,
        password: str,
        driver: str,
        classpath: str | None = None,
        properties: str | None = None,
    ) -> str:
        """Create (if absent) a named SQL Gateway JDBC connection in %SYS.

        Persisted via ``%Library.sys_SQLConnection``; returns the connection name.
        """
        conn_name = f"irispark_jdbc_{uuid.uuid4().hex[:10]}"
        if password:
            warnings.warn(
                "JDBC password is stored in plain text in IRIS's %Library.sys_SQLConnection "
                "(IRIS 2026.2 has no credential vault for SQL Gateway connections). "
                "Pass a pre-created named connection via the 'connection' argument to avoid this.",
                UserWarning,
                stacklevel=3,
            )
        cfg = self._session._config
        # Named connections live in the %SYS namespace; open a dedicated client.
        import iris as _iris

        conn = _iris.connect(
            hostname=cfg["host"], port=cfg["port"], namespace="%SYS",
            username=cfg["username"], password=cfg["password"],
        )
        try:
            cur = conn.cursor()
            cols = ["Connection_Name", "URL", "Usr", "pwd", "driver", "classpath", "isJDBC"]
            vals = [_quote(conn_name), _quote(url), _quote(user), _quote(password),
                    _quote(driver), _quote(classpath or ""), "1"]
            # NOTE: passing an empty-string 'properties' makes the IRIS JDBC
            # gateway NPE during schema import (SQLCODE -237 / NullPointer in
            # ConcurrentHashMap). Only set it when actually provided.
            if properties:
                cols.append("properties")
                vals.append(_quote(properties))
            insert = (
                "INSERT INTO %Library.sys_SQLConnection ("
                + ", ".join(cols) + ") VALUES (" + ", ".join(vals) + ")"
            )
            cur.execute(insert)
        finally:
            conn.close()
        return conn_name

    def _find_server(self, connection: str, dbtable: str) -> str | None:
        """Return an existing session foreign server for connection+dbtable, if any."""
        for srv, cfg in self._session._foreign_server_config.items():
            if cfg.get("connection") == connection and cfg.get("dbtable") == dbtable:
                return srv
        return None

    def register_file_foreign_table(
        self,
        path: str,
        format: str = "csv",
        name: str | None = None,
        options: dict[str, Any] | None = None,
        persistent: bool = False,
        server_path: str | None = None,
    ) -> IrisDataFrame:
        """Register a file-backed IRIS Foreign Table and return a DataFrame.

        Creates an IRIS Foreign Server (``FOREIGN DATA WRAPPER CSV HOST ...``)
        for the file's directory — reusing one when it already exists — and a
        Foreign Table over the file. The table is tracked in the session and
        dropped on close unless ``persistent`` is True.

        Args:
            path: File path as seen by THIS process (used for client-side
                schema inference). Supports local paths.
            format: File format. Only ``csv`` is supported by IRIS foreign
                tables (RSQL_createserver); anything else raises ValueError.
            name: Optional explicit IRIS foreign table name. If omitted, a
                unique name is generated.
            options: CSV options mapped into the USING clause's
                ``{"from": {"file": ...}}`` tree (e.g. ``header``,
                ``columnseparator``, ``escapechar``).
            persistent: If True, the foreign table is NOT added to the session's
                transient cleanup list and survives ``session.close()``.
            server_path: Directory containing the file **as seen by the IRIS
                server**, used as the CREATE FOREIGN SERVER HOST. Defaults to
                ``path``'s directory. Set this when the client and server run
                in different containers with a shared volume mounted at
                different paths.

        Returns:
            IrisDataFrame backed by the newly created file foreign table.

        Raises:
            ValueError: If ``format`` is unsupported or ``path`` is not a file
                path that can be split into a base location and a file name.
        """
        from .dataframe import IrisDataFrame
        from .session import _quote_ident
        from .sql_generator import _validate_identifier

        format = format.lower()
        if format not in _FILE_FORMAT_TO_WRAPPER:
            raise ValueError(
                f"Formato invalido: {format!r}. "
                f"Valores aceitos: {', '.join(_FILE_FORMAT_TO_WRAPPER)}"
            )

        if name is None:
            name = f"irispark_fftbl_{uuid.uuid4().hex[:12]}"
        _validate_identifier("name", name)

        server_dir, file_name = _split_file_path(path)
        if not file_name:
            raise ValueError(
                f"Caminho invalido para foreign table: {path!r}. "
                f"Forneca o caminho completo de um arquivo."
            )
        # Directory as seen by the IRIS server (HOST clause); defaults to the
        # client-side directory when both share a filesystem.
        host_dir = server_path if server_path is not None else server_dir

        col_schema = _infer_file_schema(path, format)
        col_defs = [f"{_quote_ident(col)} {iris_type}" for col, iris_type in col_schema]

        options = dict(options) if options else {}
        using_file: dict[str, Any] = {
            k: v for k, v in options.items() if k in _USING_FILE_OPTIONS
        }
        unknown = set(options) - set(using_file)
        if unknown:
            warnings.warn(
                f"Opcoes ignoradas para foreign table CSV: {sorted(unknown)}. "
                f"Suportadas: {sorted(_USING_FILE_OPTIONS)}",
                UserWarning,
                stacklevel=2,
            )

        server_name = self._find_file_server(host_dir)
        if server_name is None:
            server_name = f"irispark_fsrv_{uuid.uuid4().hex[:12]}"
            create_server_sql = (
                f"CREATE FOREIGN SERVER {_quote_ident(server_name)} "
                f"FOREIGN DATA WRAPPER {_FILE_FORMAT_TO_WRAPPER[format]} "
                f"HOST {_quote(host_dir)}"
            )
            self._session.sql(create_server_sql)

        drop_table_sql = f"DROP FOREIGN TABLE IF EXISTS {_quote_ident(name)}"
        create_table_sql = (
            f"CREATE FOREIGN TABLE {_quote_ident(name)} ({', '.join(col_defs)}) "
            f"SERVER {_quote_ident(server_name)} "
            f"FILE {_quote(file_name)}"
        )
        if using_file:
            using_json = json.dumps({"from": {"file": using_file}})
            create_table_sql += f" USING {using_json}"

        try:
            self._session.sql(drop_table_sql)
        except Exception:
            pass

        self._session.sql(create_table_sql)

        self._session._foreign_table_servers[name] = server_name
        self._session._foreign_server_config[server_name] = {
            "path": host_dir,
            "format": format,
        }
        if not persistent:
            self._session._foreign_tables.append(name)

        df = IrisDataFrame(session=self._session, table_name=name)
        df._schema = col_schema
        return df

    def _find_file_server(self, path: str) -> str | None:
        """Return an existing file foreign server for the same path, if any."""
        for srv, cfg in self._session._foreign_server_config.items():
            if cfg.get("path") == path:
                return srv
        return None

    def create_foreign_table_from_query(
        self,
        name: str,
        server_name: str,
        remote_table: str,
        select_sql: str,
        persistent: bool = False,
    ) -> IrisDataFrame:
        """Create a foreign table whose content is defined by an IRIS SELECT query.

        This is used for write-back: IRIS evaluates the SELECT inside IRIS and
        pushes the resulting rows through the foreign server into ``remote_table``.

        Args:
            name: IRIS foreign table name.
            server_name: Existing foreign server name.
            remote_table: Remote target table name.
            select_sql: IRIS SELECT query that produces rows for the target.
            persistent: If True, the foreign table survives ``session.close()``.

        Returns:
            IrisDataFrame backed by the new foreign table.
        """
        from .dataframe import IrisDataFrame
        from .session import _quote_ident
        from .sql_generator import _validate_identifier

        _validate_identifier("name", name)
        _validate_identifier("server_name", server_name)
        _validate_identifier("remote_table", remote_table)

        drop_table_sql = f"DROP FOREIGN TABLE IF EXISTS {_quote_ident(name)}"
        create_table_sql = (
            f"CREATE FOREIGN TABLE {_quote_ident(name)} "
            f"SERVER {_quote_ident(server_name)} TABLE {_quote(remote_table)} "
            f"AS {select_sql}"
        )

        try:
            self._session.sql(drop_table_sql)
        except Exception:
            pass

        self._session.sql(create_table_sql)

        self._session._foreign_table_servers[name] = server_name
        # We don't have full server config here (url/user/etc) since this helper
        # runs against an existing server; record minimal metadata for introspection.
        self._session._foreign_server_config.setdefault(server_name, {})
        if not persistent:
            self._session._foreign_tables.append(name)

        df = IrisDataFrame(session=self._session, table_name=name)
        return df

    def drop_foreign_table(self, name: str) -> None:
        """Drop a foreign table registered by this session and its server if unused."""
        from .session import _quote_ident

        transient = getattr(self._session, "_foreign_tables", [])
        if name not in transient and name not in self._session._foreign_table_servers:
            return

        server_name = self._session._foreign_table_servers.pop(name, None)
        try:
            self._session.sql(f"DROP FOREIGN TABLE IF EXISTS {_quote_ident(name)}")
        except Exception:
            pass

        if name in transient:
            self._session._foreign_tables.remove(name)

        if server_name and server_name not in self._session._foreign_table_servers.values():
            try:
                self._session.sql(f"DROP FOREIGN SERVER IF EXISTS {_quote_ident(server_name)}")
            except Exception:
                pass
            self._session._foreign_server_config.pop(server_name, None)


def _split_file_path(path: str) -> tuple[str, str]:
    """Split a file path into a base location and a file name.

    Local paths use ``os.path``; S3-style URIs use POSIX semantics so the
    server path always ends in ``/`` and can be reused across files in the
    same prefix.
    """
    if path.startswith("s3://"):
        # Preserve the s3:// prefix and split on the last slash.
        if "/" not in path[5:]:
            return path, ""
        base, _, file_name = path.rpartition("/")
        if base.endswith(":"):
            # s3://bucket/file -> keep bucket slash.
            return f"{base}/", file_name
        return f"{base}/", file_name

    base = os.path.dirname(path)
    file_name = os.path.basename(path)
    if not base:
        base = "."
    return f"{base}{os.sep}", file_name


def _infer_file_schema(path: str, format: str) -> list[tuple[str, str]]:
    """Infer an IRIS column schema from a local or S3 file.

    For Parquet the file schema is read directly. For CSV a small sample is
    parsed via pyarrow so column names and types are available for the foreign
    table DDL.
    """
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq

    from .read import _arrow_to_iris_type

    if format == "parquet":
        schema = pq.read_schema(path)
        return [(field.name, _arrow_to_iris_type(field.type)) for field in schema]

    if format == "csv":
        reader = pcsv.open_csv(path)
        schema = reader.schema
        return [(field.name, _arrow_to_iris_type(field.type)) for field in schema]

    # JSON and unknown formats: fall back to VARCHAR columns. The caller should
    # provide a typed schema when IRIS supports structured file wrappers.
    return [("value", "VARCHAR(4000)")]


def _quote(value: str) -> str:
    """Return a single-quoted SQL string literal."""
    return f"'{value.replace(chr(39), chr(39) + chr(39))}'"
