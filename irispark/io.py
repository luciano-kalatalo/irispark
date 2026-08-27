from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

from .dataframe import IrisDataFrame


def _get_session(session: Any = None) -> Any:
    """Resolve the session for a module-level I/O call.

    ``pandas-on-Spark`` reads the global default session; irispark sessions are
    transient (each ``IrisParkSession.close()`` clears the active session), so
    callers may pass an explicit ``session=``. When omitted, the active session
    is used.

    Returns:
        The resolved IrisParkSession.

    Raises:
        RuntimeError: If no session is provided and none is active.
    """
    if session is not None:
        return session
    from .session import get_active_session

    session = get_active_session()
    if session is None:
        raise RuntimeError(
            "No active IrisParkSession. Create a session first, e.g. "
            "IrisParkSession(host=..., port=..., namespace=..., ...), or "
            "pass session=<your session> explicitly."
        )
    return session


def read_csv(
    path: str,
    foreign: bool = False,
    name: str | None = None,
    persistent: bool = False,
    options: dict[str, Any] | None = None,
    session: Any = None,
) -> IrisDataFrame:
    """Read a CSV file into an :class:`IrisDataFrame` (pandas-on-Spark ``read_csv`` alias).

    Args:
        path: Local or foreign-server file path.
        foreign: Register an IRIS file foreign table instead of local ingestion.
        name: Foreign table name (when ``foreign=True``).
        persistent: Persist the foreign table (when ``foreign=True``).
        options: Reader options (e.g. ``{"HEADER": True}`` for server-side paths).

    Returns:
        IrisDataFrame backed by the loaded CSV data.
    """
    session = _get_session(session)
    return session.read.csv(
        path, foreign=foreign, name=name, persistent=persistent, options=options
    )


def read_json(
    path: str,
    foreign: bool = False,
    name: str | None = None,
    persistent: bool = False,
    options: dict[str, Any] | None = None,
    session: Any = None,
) -> IrisDataFrame:
    """Read a JSON file into an :class:`IrisDataFrame` (pandas-on-Spark ``read_json`` alias).

    Args:
        path: Local or foreign-server file path.
        foreign: Register an IRIS file foreign table instead of local ingestion.
        name: Foreign table name (when ``foreign=True``).
        persistent: Persist the foreign table (when ``foreign=True``).
        options: Reader options for server-side paths.
        session: Explicit session; defaults to the active session (see
            :func:`_get_session`).

    Returns:
        IrisDataFrame backed by the loaded JSON data.
    """
    session = _get_session(session)
    return session.read.json(
        path, foreign=foreign, name=name, persistent=persistent, options=options
    )


def read_parquet(
    path: str,
    foreign: bool = False,
    name: str | None = None,
    persistent: bool = False,
    options: dict[str, Any] | None = None,
    session: Any = None,
) -> IrisDataFrame:
    """Read a Parquet file into an :class:`IrisDataFrame` (pandas-on-Spark ``read_parquet`` alias).

    Args:
        path: Local or foreign-server file path.
        foreign: Register an IRIS file foreign table instead of local ingestion.
        name: Foreign table name (when ``foreign=True``).
        persistent: Persist the foreign table (when ``foreign=True``).
        options: Reader options for server-side paths.
        session: Explicit session; defaults to the active session (see
            :func:`_get_session`).

    Returns:
        IrisDataFrame backed by the loaded Parquet data.
    """
    session = _get_session(session)
    return session.read.parquet(
        path, foreign=foreign, name=name, persistent=persistent, options=options
    )


def read_table(
    path: str,
    format: str | None = None,
    session: Any = None,
    **options: Any,
) -> IrisDataFrame:
    """Read a data file into an :class:`IrisDataFrame` (pandas-on-Spark ``read_table`` alias).

    ``format`` may be ``"csv"``, ``"parquet"``, or ``"json"``; when omitted it is
    inferred from the file extension.

    Args:
        path: File path.
        format: File format (``csv``, ``parquet``, ``json``) or ``None`` to infer.
        **options: Forwarded to the underlying reader.

    Returns:
        IrisDataFrame backed by the loaded file data.

    Raises:
        ValueError: If the format cannot be inferred or is not supported.
    """
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    fmt = (format or ext or "").lower()
    readers = {"csv": read_csv, "parquet": read_parquet, "json": read_json}
    reader: Callable[..., IrisDataFrame] | None
    if fmt in ("pq", "parquet"):
        reader = read_parquet
    else:
        reader = readers.get(fmt)
    if reader is None:
        raise ValueError(
            f"Cannot infer format for '{path}'. Specify format="
            f"'csv' | 'parquet' | 'json'."
        )
    return reader(path, session=session, **options)


def _sql_to_dataframe(session: Any, sql: str) -> IrisDataFrame:
    """Materialize a query into a tracked temp table-backed DataFrame."""
    import uuid

    from .dataframe import IrisDataFrame

    tbl = f"irispark_sql_{uuid.uuid4().hex[:12]}"
    session.sql(f"CREATE TABLE {tbl} AS {sql}")
    session._tmp_tables.append(tbl)
    df = IrisDataFrame(session=session, table_name=tbl)
    df._append_lineage("source", f"read_sql: {sql}")
    return df


def read_sql(sql: str, index_col: str | None = None, session: Any = None) -> IrisDataFrame:
    """Execute a SQL query and return the result (pandas-on-Spark ``read_sql`` alias).

    The query result is materialized into a tracked temp table; the returned
    DataFrame is backed by it.

    Args:
        sql: The SQL query text.
        index_col: Ignored (irispark DataFrames are row-based).
        session: Explicit session; defaults to the active session (see
            :func:`_get_session`).

    Returns:
        IrisDataFrame with the query result.
    """
    session = _get_session(session)
    return _sql_to_dataframe(session, sql)


def read_sql_query(sql: str, index_col: str | None = None, session: Any = None) -> IrisDataFrame:
    """Execute a SQL query and return the result (pandas-on-Spark ``read_sql_query`` alias).

    The query result is materialized into a tracked temp table; the returned
    DataFrame is backed by it.

    Args:
        sql: The SQL query text.
        index_col: Ignored (irispark DataFrames are row-based).
        session: Explicit session; defaults to the active session (see
            :func:`_get_session`).

    Returns:
        IrisDataFrame with the query result.
    """
    session = _get_session(session)
    return _sql_to_dataframe(session, sql)


def read_sql_table(
    table_name: str, schema: str | None = None, index_col: str | None = None, session: Any = None
) -> IrisDataFrame:
    """Read a SQL table (pandas-on-Spark ``read_sql_table`` alias).

    Args:
        table_name: Table name (optionally schema-qualified).
        schema: Optional schema prefix (e.g. ``SQLUser``).
        index_col: Ignored (irispark DataFrames are row-based).

    Returns:
        IrisDataFrame over the table.
    """
    session = _get_session(session)
    if schema:
        table_name = f"{schema}.{table_name}"
    return session.table(table_name)


def from_pandas(pdf: Any, session: Any = None) -> IrisDataFrame:
    """Create an :class:`IrisDataFrame` from a pandas DataFrame (``from_pandas`` alias).

    Args:
        pdf: pandas DataFrame (or polars/dask object supported by createDataFrame).
        session: Explicit session; defaults to the active session (see
            :func:`_get_session`).

    Returns:
        IrisDataFrame backed by a temp table.
    """
    session = _get_session(session)
    return session.createDataFrame(pdf)
