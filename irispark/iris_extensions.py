from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .dataframe import IrisDataFrame


def _ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _split_table_name(table_name: str) -> tuple[str | None, str]:
    """Split ``schema.table`` into (schema, table); table-only names return (None, table)."""
    if "." in table_name:
        schema, sep, rest = table_name.partition(".")
        if sep and schema and rest:
            return schema, rest
    return None, table_name


def _infer_index_type(index_name: str, is_primary: bool) -> str:
    """Infer index type from naming conventions.

    IRIS 2026.x does not expose an index-type column in
    INFORMATION_SCHEMA.INDEXES, so ``..._bmp`` => bitmap, ``..._col`` => columnar
    and the PRIMARY_KEY flag => primary; otherwise ``"unknown"``.
    """
    if is_primary:
        return "primary"
    name = (index_name or "").lower()
    if name.endswith("_bmp"):
        return "bitmap"
    if name.endswith("_col"):
        return "columnar"
    return "unknown"


def _extract_lineage_columns(
    lineage,
) -> tuple[set[str], set[str], set[str], set[str]]:
    """Map lineage entries to filter / equality-filter / group-by / order-by columns."""
    filter_cols: set[str] = set()
    equality_cols: set[str] = set()
    groupby_cols: set[str] = set()
    orderby_cols: set[str] = set()
    word_re = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b")
    for entry in lineage or []:
        if not isinstance(entry, dict):
            continue
        op = entry.get("op", "")
        desc = entry.get("desc", "")
        if not desc:
            continue
        words = set(word_re.findall(desc))
        if op == "filter":
            filter_cols.update(words)
            if "=" in desc or " in (" in desc.lower():
                equality_cols.update(words)
        elif op in ("orderBy", "order_by"):
            orderby_cols.update(words)
        elif op in ("agg", "groupBy", "group_by"):
            if "group" in desc.lower():
                groupby_cols.update(words)
    return filter_cols, equality_cols, groupby_cols, orderby_cols


def _rank_index_candidates(
    columns: Iterable[str],
    filter_cols: Iterable[str],
    equality_cols: Iterable[str],
    groupby_cols: Iterable[str],
    orderby_cols: Iterable[str],
    cardinality: Mapping[str, int] | None = None,
    row_count: int | None = None,
) -> dict[str, list[str]]:
    """Rank index candidates from query shape and optional column cardinality.

    Columnar: range-filtered, group-by, and order-by columns.
    Bitmap: equality-filtered columns with low cardinality
    (distinct <= max(100, 10% of rows)) when cardinality data is available,
    otherwise all equality-filtered columns.

    Never recommends columns absent from ``columns`` and never falls back to a
    blanket all-columns suggestion.
    """
    col_set = set(columns)
    if not col_set:
        return {"columnar": [], "bitmap": []}
    filter_set = set(filter_cols) & col_set
    equality_set = set(equality_cols) & col_set
    group_set = set(groupby_cols) & col_set
    order_set = set(orderby_cols) & col_set

    columnar = sorted((filter_set - equality_set) | group_set | order_set)
    bitmap_pool = equality_set - group_set - order_set
    if cardinality is not None:
        threshold = max(100.0, (row_count or 0) * 0.1)
        bitmap = [c for c in sorted(bitmap_pool) if cardinality.get(c, float("inf")) <= threshold]
    else:
        bitmap = sorted(bitmap_pool)

    return {"columnar": columnar, "bitmap": bitmap}


class IrisExtensions:
    """IRIS-specific extensions for DataFrame operations.

    Provides access to IRIS-specific features that don't have
    direct PySpark equivalents.

    Usage:
        df.iris.createColumnarIndex("column_name")
        df.iris.show_stats()
        df.iris.show_indexes()
        df.iris.suggest_indexes()
    """

    def __init__(self, df: IrisDataFrame) -> None:
        self._df = df

    def explain(self) -> list[str]:
        """Return IRIS execution plan as structured output.

        Returns:
            List of strings representing the execution plan lines.
        """
        sql = self._df.to_sql()
        rows, _ = self._df.session.sql(f"EXPLAIN {sql}")
        return [str(r[0]) for r in rows]

    def show_stats(self) -> dict:
        """Return table statistics: row count, column count, and storage size.

        ``storage_bytes`` and ``block_count`` are included only when the server
        exposes the ``%SYS.GlobalQuery_Namespace`` SQL API; IRIS 2026.x removed
        it, so the keys are omitted on such servers rather than raising.

        Returns:
            Dictionary with table statistics.
        """
        tbl = self._df.table_name
        rows, _ = self._df.session.sql(f"SELECT COUNT(*) FROM {tbl}")
        row_count = rows[0][0] if rows else 0
        _, col_names = self._df.session.sql(f"SELECT * FROM {tbl} LIMIT 0")
        col_count = len(col_names) if col_names else 0
        stats: dict[str, object] = {"row_count": row_count, "column_count": col_count}
        storage = self._storage_estimate(tbl)
        if storage:
            stats.update(storage)
        return stats

    def _storage_estimate(self, tbl: str) -> dict[str, int] | None:
        """Best-effort byte/block estimate via %SYS.GlobalQuery_Namespace (pre-2026 IRIS only)."""
        try:
            rows, _ = self._df.session.sql(
                f"SELECT GlobalSize "
                f"FROM %SYS.GlobalQuery_Namespace('', '^{tbl}.*', '', 1)"
            )
            total = sum(int(r[0]) for r in rows if r[0] is not None)
            if total > 0:
                return {"storage_bytes": total, "block_count": math.ceil(total / 8192)}
        except Exception:
            pass
        return None

    def show_indexes(self) -> list[dict]:
        """List indexes on the table.

        Returns:
            List of dictionaries with index name, type, and column(s).
        """
        from .column import _quote
        from .sql_generator import _validate_identifier
        _validate_identifier("table_name", self._df.table_name)
        indexes: list[dict] = []

        try:
            schema, table = _split_table_name(self._df.table_name)
            where = [f"TABLE_NAME = {_quote(table)}"]
            if schema:
                where.append(f"TABLE_SCHEMA = {_quote(schema)}")
            sql = (
                "SELECT INDEX_NAME, NON_UNIQUE, ORDINAL_POSITION, COLUMN_NAME, PRIMARY_KEY "
                "FROM INFORMATION_SCHEMA.INDEXES "
                f"WHERE {' AND '.join(where)} "
                "ORDER BY INDEX_NAME, ORDINAL_POSITION"
            )
            rows, _ = self._df.session.sql(sql)
            if rows:
                idx_map: dict[str, dict] = {}
                for row in rows:
                    idx_name = row[0] if isinstance(row, (list, tuple)) else row["INDEX_NAME"]
                    non_unique = row[1] if isinstance(row, (list, tuple)) else row["NON_UNIQUE"]
                    col_name = row[3] if isinstance(row, (list, tuple)) else row["COLUMN_NAME"]
                    is_primary = row[4] if isinstance(row, (list, tuple)) else row["PRIMARY_KEY"]

                    if idx_name not in idx_map:
                        idx_map[idx_name] = {
                            "name": idx_name,
                            "type": _infer_index_type(idx_name, bool(is_primary)),
                            "columns": [],
                            "unique": not bool(non_unique)
                        }
                    if col_name and col_name not in idx_map[idx_name]["columns"]:
                        idx_map[idx_name]["columns"].append(col_name)

                indexes = list(idx_map.values())
        except Exception:
            # Fall back to execution plan parsing if INFORMATION_SCHEMA fails
            pass

        # Fall back to execution plan parsing if no indexes found via INFORMATION_SCHEMA
        if not indexes:
            try:
                plan = self._df.iris.explain()
                for line in plan:
                    if "COLUMN" in str(line).upper() and "INDEX" in str(line).upper():
                        import re
                        match = re.search(r'idx_\w+_\w+_col', str(line))
                        if match:
                            idx_name = match.group(0)
                            indexes.append({
                                "name": idx_name,
                                "type": "columnar",
                                "columns": [self._df.table_name]
                            })
            except Exception:
                pass

            if not indexes:
                try:
                    plan = self._df.iris.explain()
                    for line in plan:
                        if "BITMAP" in str(line).upper():
                            import re
                            match = re.search(r'idx_\w+_\w+_bmp', str(line))
                            if match:
                                idx_name = match.group(0)
                                # Avoid duplicates
                                if not any(i["name"] == idx_name for i in indexes):
                                    indexes.append({
                                        "name": idx_name,
                                        "type": "bitmap",
                                        "columns": [self._df.table_name]
                                    })
                except Exception:
                    pass

        return indexes

    def suggest_indexes(self) -> dict:
        """Suggest columnar and bitmap indexes from query lineage and column cardinality.

        Returns:
            Dictionary with ``"columnar"`` and ``"bitmap"`` column name lists.
        """
        tbl = self._df.table_name
        col_names: set[str] = set()
        try:
            _, cols_schema = self._df.session.sql(f"SELECT * FROM {tbl} LIMIT 0")
            col_names = set(cols_schema) if cols_schema else set()
        except Exception:
            pass

        filter_cols, equality_cols, groupby_cols, orderby_cols = _extract_lineage_columns(
            self._df.lineage()
        )

        row_count: int | None = None
        cardinality: dict[str, int] | None = None
        try:
            rows, _ = self._df.session.sql(f"SELECT COUNT(*) FROM {tbl}")
            row_count = int(rows[0][0]) if rows else 0
        except Exception:
            pass
        if row_count is not None:
            cardinality = self._measure_cardinality(tbl, equality_cols & col_names)

        return _rank_index_candidates(
            col_names,
            filter_cols,
            equality_cols,
            groupby_cols,
            orderby_cols,
            cardinality,
            row_count,
        )

    def _measure_cardinality(self, tbl: str, columns: set[str]) -> dict[str, int]:
        result: dict[str, int] = {}
        for col in sorted(columns):
            try:
                rows, _ = self._df.session.sql(
                    f"SELECT COUNT(DISTINCT {_ident(col)}) FROM {tbl}"
                )
                result[col] = int(rows[0][0]) if rows else 0
            except Exception:
                continue
        return result

    def createColumnarIndex(self, column: str) -> None:
        """Create a columnar index on a row-stored table column.

        Columnar indexes store vectorized column data and can significantly
        improve analytical query performance on row-stored tables.

        Args:
            column: Name of the column to index.
        """
        tbl = self._df.table_name
        idx_name = f"idx_{tbl}_{column}_col"
        self._df.session.sql(f"CREATE COLUMNAR INDEX {idx_name} ON {tbl}({column})")

    def createBitmapIndex(self, column: str) -> None:
        """Create a bitmap index on a table column.

        Bitmap indexes are useful for low-cardinality columns in analytical queries.

        Args:
            column: Name of the column to index.
        """
        tbl = self._df.table_name
        idx_name = f"idx_{tbl}_{column}_bmp"
        self._df.session.sql(f"CREATE BITMAP INDEX {idx_name} ON {tbl}({column})")

    def tableStats(self) -> dict:
        """Return basic table statistics.

        Returns:
            Dictionary with table statistics (currently row count).
        """
        tbl = self._df.table_name
        rows, _ = self._df.session.sql(f"SELECT COUNT(*) FROM {tbl}")
        return {"row_count": rows[0][0] if rows else 0}

    @property
    def foreign(self) -> IrisForeignExtensions:
        """Access foreign-table specific helpers for this DataFrame."""
        return IrisForeignExtensions(self._df)


class IrisForeignExtensions:
    """Foreign-table helpers for an IrisDataFrame."""

    def __init__(self, df: IrisDataFrame) -> None:
        self._df = df

    def is_foreign_table(self) -> bool:
        """Return True if this DataFrame is backed by a session foreign table."""
        return self._df.table_name in getattr(self._df.session, "_foreign_tables", []) or self._df.table_name in self._df.session._foreign_table_servers

    def server_name(self) -> str | None:
        """Return the foreign server name for this foreign table, if any."""
        return self._df.session._foreign_table_servers.get(self._df.table_name)

    def is_persistent(self) -> bool:
        """Return True if the foreign table is not in the session transient list."""
        return self.is_foreign_table() and self._df.table_name not in getattr(
            self._df.session, "_foreign_tables", []
        )

    def refresh(self) -> None:
        """Refresh metadata by recreating the underlying foreign table.

        This is a no-op for non-foreign tables.
        """
        if not self.is_foreign_table():
            return
        # Re-create the foreign table via the session helper; the original
        # registration parameters are not retained, so callers should use
        # session.iris.register_jdbc_foreign_table() for full re-registration.
        self._df.session.iris.drop_foreign_table(self._df.table_name)
