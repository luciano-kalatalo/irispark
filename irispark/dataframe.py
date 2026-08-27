from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from .column import Column, SortColumn
from .grouped import GroupedData
from .row import Row
from .sql_generator import (
    SQLGenerator,
    _quote_if_reserved,
    _serialize,
    _validate_identifier,
)
from .writer import DataFrameWriter

if TYPE_CHECKING:
    from .iris_extensions import IrisExtensions
    from .session import IrisParkSession
    from .types import DataType, StructType


_TYPE_CODE_MAP: dict[int, str] = {
    4: "integer",
    -5: "bigint",
    5: "smallint",
    -6: "tinyint",
    6: "float",
    8: "double",
    3: "decimal",
    12: "string",
    -1: "string",
    1: "string",
    -8: "string",
    -9: "string",
    16: "boolean",
    91: "date",
    93: "timestamp",
    -2: "binary",
    0: "null",
    1091: "date",
}

_TYPE_NAME_MAP: dict[str, str] = {
    "INT": "integer",
    "INTEGER": "integer",
    "BIGINT": "bigint",
    "SMALLINT": "smallint",
    "TINYINT": "tinyint",
    "DOUBLE": "double",
    "FLOAT": "float",
    "REAL": "float",
    "NUMERIC": "decimal",
    "DECIMAL": "decimal",
    "VARCHAR": "string",
    "CHAR": "string",
    "STRING": "string",
    "BOOLEAN": "boolean",
    "BIT": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "DATETIME": "timestamp",
    "BINARY": "binary",
}

_NAME_TO_TYPE_CODE: dict[str, int] = {
    "integer": 4,
    "bigint": -5,
    "smallint": 5,
    "tinyint": -6,
    "float": 6,
    "double": 8,
    "decimal": 3,
    "string": 12,
    "boolean": 16,
    "date": 91,
    "timestamp": 93,
    "binary": -2,
    "null": 0,
}


def _normalize_replace_pairs(to_replace: Any, value: Any) -> list[tuple[Any, Any]]:
    """Normalize ``na.replace`` arguments into (old, new) pairs.

    Supports the PySpark forms: scalar→scalar, mapping, and parallel lists.
    Raises ``ValueError`` for mismatched or missing counterparts, mirroring
    PySpark's argument validation.
    """
    if isinstance(to_replace, dict):
        if value is not None:
            raise ValueError("value cannot be set when to_replace is a dict")
        return list(to_replace.items())
    if isinstance(to_replace, (list, tuple)):
        if not isinstance(value, (list, tuple)):
            raise ValueError("value must be a list when to_replace is a list")
        if len(to_replace) != len(value):
            raise ValueError(
                f"to_replace and value must have the same length: "
                f"{len(to_replace)} != {len(value)}"
            )
        return list(zip(to_replace, value))
    if isinstance(value, (list, tuple)):
        raise ValueError("to_replace must be a list when value is a list")
    return [(to_replace, value)]


def _fillna_type_compatible(value: Any, type_name: str) -> bool:
    """Return True if ``value`` can fill a column of the given IRIS type.

    Mirrors PySpark ``fillna``: when ``subset`` is omitted, only columns whose
    type is compatible with the value are filled — a string fills string
    columns, a number fills numeric columns. This avoids emitting
    ``COALESCE(int_col, 'string')`` which IRIS rejects (SQLCODE -378).
    """

    base = _normalize_type(type_name)
    if isinstance(value, str):
        return base == "string"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return base in {"integer", "bigint", "smallint", "tinyint", "float", "double", "decimal"}
    return True


def _normalize_type(t: Any) -> str:
    """Return the PySpark-style type name for a schema entry.

    Accepts either a JDBC type code (int) or an IRIS type name (str) such as
    ``VARCHAR(4000)`` or ``NUMERIC(38,2)``, as stored by ``createDataFrame``.
    """
    if isinstance(t, int):
        return _TYPE_CODE_MAP.get(t, "string")
    name = str(t).upper()
    for prefix in ("VARCHAR", "CHAR", "DECIMAL", "NUMERIC"):
        if name.startswith(prefix):
            name = prefix
            break
    base = name.split("(")[0].strip()
    return _TYPE_NAME_MAP.get(base, "string")


def _type_code_to_name(code: Any) -> str:
    return _normalize_type(code)


def _type_code_to_datatype(code: int) -> DataType:
    from .types import (
        BinaryType,
        BooleanType,
        ByteType,
        DateType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        NullType,
        ShortType,
        StringType,
        TimestampType,
    )
    mapping: dict[int, DataType] = {
        4: IntegerType(),
        -5: LongType(),
        5: ShortType(),
        -6: ByteType(),
        6: FloatType(),
        8: DoubleType(),
        3: DecimalType(),
        12: StringType(),
        -1: StringType(),
        1: StringType(),
        -8: StringType(),
        -9: StringType(),
        16: BooleanType(),
        91: DateType(),
        93: TimestampType(),
        -2: BinaryType(),
        0: NullType(),
        1091: DateType(),
    }
    if isinstance(code, int):
        return mapping.get(code, StringType())
    return mapping.get(_NAME_TO_TYPE_CODE.get(_normalize_type(code), -1), StringType())


_JOIN_MAP: dict[str, str] = {
    "inner": "INNER JOIN",
    "full": "FULL OUTER JOIN",
    "fullouter": "FULL OUTER JOIN",
    "outer": "FULL OUTER JOIN",
    "left": "LEFT JOIN",
    "leftouter": "LEFT JOIN",
    "right": "RIGHT JOIN",
    "rightouter": "RIGHT JOIN",
    "leftsemi": "LEFT SEMI",
    "leftanti": "LEFT ANTI",
}


def _normalize_join_type(how: str) -> str:
    key = how.lower().replace("_", "")
    return _JOIN_MAP.get(key, how.upper())


_SUMMARY_STATS = ["count", "mean", "stddev", "min", "max"]


class IrisDataFrame:
    _schema: list[tuple[str, Any]]

    def __init__(
        self,
        session: IrisParkSession,
        table_name: str,
        select_cols: list[Any] | None = None,
        filters: list[Any] | None = None,
        group_cols: list[Any] | None = None,
        aggregations: dict[Any, str] | None = None,
        limit_n: int | None = None,
        order_cols: list[Any] | None = None,
        with_columns: list[tuple[str, Column]] | None = None,
        join_config: list[dict[str, Any]] | None = None,
        distinct: bool = False,
        dropped_cols: list[str] | None = None,
        union_parts: list[IrisDataFrame] | None = None,
        renamed_cols: dict[str, str] | None = None,
        fillna_values: dict[str, Any] | None = None,
        replace_values: dict[str, list[tuple[Any, Any]]] | None = None,
        grouped_base_columns: list[tuple[str, Column]] | None = None,
        drop_duplicates_subset: list[str] | None = None,
        sample_fraction: float | None = None,
        sample_seed: int | None = None,
        sample_with_replacement: bool = False,
        random_split: tuple[int, float, float] | None = None,
        agg_exprs: list[Column] | None = None,
        pivot_col: str | None = None,
        pivot_values: list[Any] | None = None,
        group_type: str = "GROUP BY",
        lineage_entries: list[dict[str, str]] | None = None,
        unpivot_config: dict[str, Any] | None = None,
    ) -> None:
        self.session = session
        self.table_name = table_name
        self.select_cols = select_cols or ["*"]
        self.filters = filters or []
        self.group_cols = group_cols or []
        self.aggregations = aggregations or {}
        self.limit_n = limit_n
        self.order_cols = order_cols or []
        self.with_columns = with_columns or []
        self.join_config = list(join_config) if join_config else []
        self._distinct = distinct
        self._dropped_cols = dropped_cols or []
        self._union_parts = list(union_parts) if union_parts else []
        self._renamed_cols = renamed_cols or {}
        self._fillna_values = fillna_values or {}
        self._replace_values: dict[str, list[tuple[Any, Any]]] = replace_values or {}
        # withColumns consumed by an aggregation: they live inside the grouped
        # base layer instead of stacking above it (see IrisDataFrame.agg).
        self._grouped_base_columns = grouped_base_columns or []
        self._drop_duplicates_subset = drop_duplicates_subset or []
        self._sample_fraction = sample_fraction
        self._sample_seed = sample_seed
        self._sample_with_replacement = sample_with_replacement
        self._random_split = random_split
        self._schema: list[tuple[str, Any]] = []
        self._base_schema: list[tuple[str, str]] | None = None
        self._agg_exprs = agg_exprs or []
        self._pivot_col = pivot_col
        self._pivot_values = list(pivot_values) if pivot_values else None
        self._group_type = group_type
        self._lineage_entries: list[dict[str, str]] = list(lineage_entries) if lineage_entries else []
        self._unpivot_config: dict[str, Any] | None = unpivot_config
        self._alias: str | None = None
        self._in_grouped_wc: bool = False

    @property
    def write(self) -> DataFrameWriter:
        return DataFrameWriter(self)

    @property
    def _table_alias(self) -> str:
        return self._alias if self._alias else self.table_name

    def __getattr__(self, name: str) -> Column:
        if name.startswith("_"):
            raise AttributeError(name)
        return Column(f"{self.table_name}.{name}")

    def __getitem__(self, item: str | list[str]) -> Column | IrisDataFrame:
        if isinstance(item, str):
            return Column(f"{self.table_name}.{item}")
        if isinstance(item, list):
            return self.select(*item)
        raise TypeError(f"Unsupported type: {type(item)}")

    def alias(self, alias: str) -> IrisDataFrame:
        df = self._copy()
        df._alias = alias
        df._append_lineage("alias", alias)
        return df

    def select(self, *cols: Any) -> IrisDataFrame:
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            select_list: list[Any] = list(cols[0])
        else:
            select_list = list(cols)
        df = self._copy(select_cols=select_list, with_columns=self.with_columns)
        df._append_lineage("select", ", ".join(str(c) for c in select_list))
        return df

    def selectExpr(self, *exprs: str) -> IrisDataFrame:
        from .functions import expr
        if len(exprs) == 1 and isinstance(exprs[0], (list, tuple)):
            exprs = exprs[0]
        df = self._copy(select_cols=[expr(e) for e in exprs], with_columns=self.with_columns)
        df._append_lineage("selectExpr", ", ".join(exprs))
        return df

    def filter(self, condition: Any) -> IrisDataFrame:
        df = self._copy(filters=self.filters + [condition])
        df._append_lineage("filter", str(condition))
        return df

    def where(self, condition: Any) -> IrisDataFrame:
        return self.filter(condition)

    def groupBy(self, *cols: Any) -> GroupedData:
        df = self._copy(group_cols=list(cols))
        return GroupedData(df)

    def group_by(self, *cols: Any) -> GroupedData:
        return self.groupBy(*cols)

    def agg(self, *exprs: Any) -> IrisDataFrame:
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            df = self._copy(
                aggregations=exprs[0],
                # Aggregation consumes the current pipeline: parent withColumns
                # materialize inside the grouped base layer; the result starts
                # clean so later withColumns stack ABOVE the aggregation.
                with_columns=[],
                grouped_base_columns=list(self.with_columns),
            )
        elif exprs:
            df = self._copy(
                agg_exprs=list(exprs),
                with_columns=[],
                grouped_base_columns=list(self.with_columns),
            )
        else:
            df = self._copy()
        desc_parts: list[str] = []
        if self.group_cols:
            desc_parts.append(f"groupBy=[{', '.join(str(c) for c in self.group_cols)}]")
        if exprs:
            if len(exprs) == 1 and isinstance(exprs[0], dict):
                desc_parts.append(str(exprs[0]))
            else:
                desc_parts.append(", ".join(str(e) for e in exprs))
        df._append_lineage("agg", "; ".join(desc_parts) if desc_parts else "no-op")
        return df

    def withColumn(self, colName: str, col: Column) -> IrisDataFrame:
        df = self._copy(with_columns=self.with_columns + [(colName, col)])
        df._append_lineage("withColumn", f"{colName} = {col._expr}")
        return df

    def order_by(self, *cols: Any) -> IrisDataFrame:
        df = self._copy(order_cols=list(cols))
        df._append_lineage("orderBy", ", ".join(str(c) for c in cols))
        return df

    def orderBy(self, *cols: Any) -> IrisDataFrame:
        return self.order_by(*cols)

    def sort(self, *cols: Any, **kwargs: Any) -> IrisDataFrame:
        return self.order_by(*cols)

    def limit(self, n: int) -> IrisDataFrame:
        df = self._copy(limit_n=n)
        df._append_lineage("limit", str(n))
        return df

    def head(self, n: int = 1) -> list[Row]:
        return self.limit(n).collect()

    def first(self) -> Row | None:
        rows = self.limit(1).collect()
        return rows[0] if rows else None

    def take(self, n: int) -> list[Row]:
        return self.limit(n).collect()

    def count(self) -> int:
        # IRIS rejects ORDER BY inside a subquery (used here as the row source
        # for COUNT), but ORDER BY does not change the row count, so strip a
        # trailing top-level ORDER BY (one not inside a window OVER(...)).
        import re as _re

        inner = self.to_sql()
        # Find the last ORDER BY that is not inside OVER(...) — i.e. not
        # followed by a closing paren for the window before it.
        m = list(_re.finditer(r"\bORDER BY\b", inner, _re.IGNORECASE))
        strip_idx = -1
        for match in m:
            tail = inner[match.end():]
            # If the remainder before the next ')' still contains an unclosed
            # ')' we are inside a window; only treat as top-level otherwise.
            if ")" not in tail:
                strip_idx = match.start()
        if strip_idx != -1:
            inner = inner[:strip_idx].rstrip()
        sql = f"SELECT COUNT(*) AS _cnt FROM ({inner}) AS _c"
        rows, _ = self.session.sql(sql)
        return int(rows[0][0]) if rows else 0

    def isEmpty(self) -> bool:
        return self.count() == 0

    def tail(self, n: int = 1) -> list[Row]:
        if n <= 0:
            return []
        sql = self.to_sql()
        if self.order_cols:
            reversed_parts: list[str] = []
            for o in self.order_cols:
                if isinstance(o, SortColumn):
                    reversed_parts.append(o._reverse_expr)
                    continue
                s = _serialize(o)
                parts = s.split(None, 1)
                col = _quote_if_reserved(parts[0])
                direction = parts[1].upper() if len(parts) > 1 else "ASC"
                rev_dir = "DESC" if direction.startswith("ASC") else "ASC"
                reversed_parts.append(f"{col} {rev_dir}")
            base = SQLGenerator(self._copy(order_cols=[])).generate()
            tail_sql = f"SELECT TOP {n} * FROM ({base}) AS _tail ORDER BY {', '.join(reversed_parts)}"
        else:
            self._ensure_schema()
            order_col = self._schema[0][0] if self._schema else "1"
            tail_sql = f"SELECT TOP {n} * FROM ({sql}) AS _tail ORDER BY {order_col} DESC"
        rows, columns = self.session.sql(tail_sql)
        col_names = [str(c) for c in columns]
        from .row import _make_row

        return [_make_row(row, col_names) for row in reversed(rows)]

    def describe(self, *cols: str) -> IrisDataFrame:
        self._ensure_schema()
        numeric_cols: list[str] = []
        for c, t in self._schema:
            if _type_code_to_name(t) in ("integer", "bigint", "smallint", "tinyint", "float", "double", "decimal"):
                numeric_cols.append(c)
        if cols:
            numeric_cols = [c for c in cols if c in [n for n, _ in self._schema]]
        if not numeric_cols:
            return self._empty_summary_df()
        stats: list[str] = []
        for col_name in numeric_cols:
            # PySpark's describe() returns every statistic as a string; casting
            # here keeps columns homogeneous so Arrow/pandas never see mixed types.
            stats.append(f"CAST(COUNT({col_name}) AS VARCHAR) AS count_{col_name}")
            stats.append(f"CAST(AVG({col_name}) AS VARCHAR) AS mean_{col_name}")
            stats.append(f"CAST(STDDEV({col_name}) AS VARCHAR) AS stddev_{col_name}")
            stats.append(f"CAST(MIN({col_name}) AS VARCHAR) AS min_{col_name}")
            stats.append(f"CAST(MAX({col_name}) AS VARCHAR) AS max_{col_name}")
        pipeline_sql = SQLGenerator(self).generate()
        sql = f"SELECT {', '.join(stats)} FROM ({pipeline_sql}) AS _desc"
        rows, columns = self.session.sql(sql)
        if not rows:
            return self._empty_summary_df()
        n_stats = len(_SUMMARY_STATS)
        result = [
            (name,) + tuple(rows[0][i] for i in range(idx, len(rows[0]), n_stats))
            for idx, name in enumerate(_SUMMARY_STATS)
        ]
        return self.session.createDataFrame(result, ["summary"] + numeric_cols)

    def summary(self, *statistics: str) -> IrisDataFrame:
        self._ensure_schema()

        if not statistics:
            statistics = ("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")

        all_cols = [c for c, _ in self._schema]
        if not all_cols:
            return self._empty_summary_df()

        numeric_cols: set[str] = set()
        for c, t in self._schema:
            type_name = _type_code_to_name(t)
            if type_name in ("integer", "bigint", "smallint", "tinyint", "float", "double", "decimal"):
                numeric_cols.add(c)

        stats_parts: list[str] = []
        for col_name in all_cols:
            # Same as describe(): cast every statistic to VARCHAR so result
            # columns are homogeneous (matches PySpark summary() semantics).
            stats_parts.append(f'CAST(COUNT("{col_name}") AS VARCHAR)')
            if col_name in numeric_cols:
                stats_parts.append(f'CAST(AVG("{col_name}") AS VARCHAR)')
                stats_parts.append(f'CAST(STDDEV("{col_name}") AS VARCHAR)')
            stats_parts.append(f'CAST(MIN("{col_name}") AS VARCHAR)')
            stats_parts.append(f'CAST(MAX("{col_name}") AS VARCHAR)')

        pipeline_sql = SQLGenerator(self).generate()
        sql = f'SELECT {", ".join(stats_parts)} FROM ({pipeline_sql}) AS _sum'
        rows, _ = self.session.sql(sql)

        if not rows:
            return self._empty_summary_df()

        row = rows[0]
        col_stats: dict[str, dict[str, Any]] = {c: {} for c in all_cols}

        idx = 0
        for col_name in all_cols:
            col_stats[col_name]["count"] = row[idx]
            idx += 1
            if col_name in numeric_cols:
                col_stats[col_name]["mean"] = row[idx]
                idx += 1
                col_stats[col_name]["stddev"] = row[idx]
                idx += 1
            col_stats[col_name]["min"] = row[idx]
            idx += 1
            col_stats[col_name]["max"] = row[idx]
            idx += 1

        need_percentiles = any(s in statistics for s in ["25%", "50%", "75%"])
        if need_percentiles:
            for col_name in numeric_cols:
                probs: list[float] = []
                if "25%" in statistics:
                    probs.append(0.25)
                if "50%" in statistics:
                    probs.append(0.50)
                if "75%" in statistics:
                    probs.append(0.75)
                if probs:
                    qs = self.approxQuantile(col_name, probs, 0.01)
                    prob_idx = 0
                    if "25%" in statistics:
                        col_stats[col_name]["25%"] = str(qs[prob_idx])
                        prob_idx += 1
                    if "50%" in statistics:
                        col_stats[col_name]["50%"] = str(qs[prob_idx])
                        prob_idx += 1
                    if "75%" in statistics:
                        col_stats[col_name]["75%"] = str(qs[prob_idx])
                        prob_idx += 1

        result_rows: list[tuple] = []
        for stat in statistics:
            row_vals: list = [stat]
            for col_name in all_cols:
                val = col_stats[col_name].get(stat)
                row_vals.append(val)
            result_rows.append(tuple(row_vals))

        return self.session.createDataFrame(result_rows, ["summary"] + all_cols)

    def _empty_summary_df(self) -> IrisDataFrame:
        return self.session.createDataFrame([], ["summary"])

    def join(self, other: IrisDataFrame, on: Any = None, how: str = "inner") -> IrisDataFrame:
        if on is None:
            return self.crossJoin(other) if how in ("inner", "cross") else (
                self._copy(join_config=self.join_config + [
                    {"right": other, "how": _normalize_join_type(how)}
                ])
            )
        if isinstance(on, str):
            on_col = self._build_join_on(other, [on])
        elif isinstance(on, list):
            on_col = self._build_join_on(other, on)
        elif isinstance(on, Column):
            on_col = on
        else:
            raise TypeError(
                "on must be a Column expression, string, or list of strings"
            )
        new_link = {"right": other, "on": on_col, "how": _normalize_join_type(how)}
        df = self._copy(join_config=self.join_config + [new_link])
        df._append_lineage("join", f"{how} on {on_col._expr}")
        return df

    def _build_join_on(self, other: IrisDataFrame, cols: list[str]) -> Column:
        left_t = self.table_name
        right_t = other.table_name
        parts = [f"{left_t}.{c} = {right_t}.{c}" for c in cols]
        return Column(" AND ".join(parts))

    def crossJoin(self, other: IrisDataFrame) -> IrisDataFrame:
        new_link = {"right": other, "how": "CROSS JOIN"}
        df = self._copy(join_config=self.join_config + [new_link])
        df._append_lineage("crossJoin", "")
        return df

    def sample(
        self,
        fraction: float,
        withReplacement: bool = False,
        seed: int | None = None,
    ) -> IrisDataFrame:
        if not 0 <= fraction <= 1:
            raise ValueError("fraction must be between 0 and 1")
        if seed is None:
            import random
            seed = random.randint(0, 2**31)
        df = self._copy(sample_fraction=fraction, sample_seed=seed, sample_with_replacement=withReplacement)
        df._append_lineage("sample", f"fraction={fraction}, seed={seed}")
        return df

    def randomSplit(self, weights: list[float], seed: int | None = None) -> list[IrisDataFrame]:
        if not weights:
            raise ValueError("weights must not be empty")
        if any(w < 0 for w in weights):
            raise ValueError("weights must be non-negative")
        total = sum(weights)
        if total <= 0:
            raise ValueError("weights must sum to a positive value")
        normalised = [w / total for w in weights]
        if seed is None:
            import random
            seed = random.randint(0, 2**31)
        # randomSplit splits via a MOD((%ID * seed) ...) filter, which requires
        # %ID to be visible. %ID is a hidden column of a physical table and is
        # NOT projected through withColumn/union subqueries, so materialize the
        # source to a temp table first when it is not already a plain table.
        if self.with_columns:
            import uuid

            tbl = f"irispark_rsplit_{uuid.uuid4().hex[:8]}"
            self.write.mode("overwrite").saveAsTable(tbl)
            base = IrisDataFrame(session=self.session, table_name=tbl)
            base._schema = list(self._schema)
        else:
            base = self
        splits: list[IrisDataFrame] = []
        lo = 0.0
        for w in normalised:
            hi = lo + w
            df = base._copy(sample_fraction=None, random_split=(seed, lo, hi))
            df._append_lineage("randomSplit", f"weight={w:.3f}, seed={seed}")
            splits.append(df)
            lo = hi
        return splits

    def coalesce(self, numPartitions: int) -> IrisDataFrame:
        df = self._copy()
        df._append_lineage("coalesce", str(numPartitions))
        return df

    def repartition(self, numPartitions: int, *cols: Any) -> IrisDataFrame:
        df = self._copy()
        df._append_lineage("repartition", str(numPartitions))
        return df

    def approxQuantile(self, col: str, probabilities: list[float], relativeError: float) -> list[float]:
        if not probabilities:
            return []
        pipeline = SQLGenerator(self).generate()
        exprs = [
            f"IRISPARK_PERCENTILE(LIST({col}), {p}) AS _q{i}"
            for i, p in enumerate(probabilities)
        ]
        sql = f"SELECT {', '.join(exprs)} FROM ({pipeline}) AS _q"
        rows, _ = self.session.sql(sql)
        if not rows:
            return [float("nan")] * len(probabilities)
        return [float(rows[0][i]) for i in range(len(probabilities))]

    def cube(self, *cols: Any) -> GroupedData:
        df = self._copy(group_cols=list(cols), group_type="CUBE")
        return GroupedData(df)

    def rollup(self, *cols: Any) -> GroupedData:
        df = self._copy(group_cols=list(cols), group_type="ROLLUP")
        return GroupedData(df)

    def unpivot(self, label_col: str, value_col: str, *cols: str) -> IrisDataFrame:
        self._ensure_schema()
        schema_names = {c for c, _ in self._schema}
        for c in cols:
            if c not in schema_names:
                raise ValueError(f"Column '{c}' not found in DataFrame")
        df = self._copy(unpivot_config={
            "label_col": label_col,
            "value_col": value_col,
            "cols": list(cols),
        })
        df._append_lineage("unpivot", f"{label_col}/{value_col} <- {', '.join(cols)}")
        return df

    def melt(self, id_vars: list[str] | None = None, value_vars: list[str] | None = None,
             var_name: str = "variable", value_name: str = "value") -> IrisDataFrame:
        self._ensure_schema()
        all_cols = [c for c, _ in self._schema]
        if id_vars is None:
            id_vars = []
        if value_vars is None:
            value_vars = [c for c in all_cols if c not in id_vars]
        if not value_vars:
            raise ValueError("No columns to unpivot")
        return self.unpivot(var_name, value_name, *value_vars)

    def explode(self, col_name: str) -> IrisDataFrame:
        self._ensure_schema()
        schema_dict = dict(self._schema)
        if col_name not in schema_dict:
            raise ValueError(f"Column '{col_name}' not found in DataFrame")

        rows, columns = self.session.sql(self.to_sql())
        col_idx = columns.index(col_name)

        exploded_rows: list[tuple[Any, ...]] = []
        for row in rows:
            val = row[col_idx]
            if val is None or (isinstance(val, str) and val.strip() == ""):
                continue
            parts = val.split(",") if isinstance(val, str) else [val]
            for part in parts:
                new_row = list(row)
                new_row[col_idx] = part.strip() if isinstance(part, str) else part
                exploded_rows.append(tuple(new_row))

        df = self.session.createDataFrame(exploded_rows, list(columns))
        df._append_lineage("explode", col_name)
        return df

    def cache(self) -> IrisDataFrame:
        import hashlib

        sql = self.to_sql()
        cache_key = hashlib.sha256(sql.encode()).hexdigest()[:16]
        if cache_key in self.session._cache_registry:
            tbl = self.session._cache_registry[cache_key]
            return IrisDataFrame(session=self.session, table_name=tbl)
        tbl = f"irispark_cache_{cache_key}"
        try:
            self.session.sql(f"DROP TABLE {tbl}")
        except Exception:
            pass
        self.session.sql(f"CREATE TABLE {tbl} AS {sql}")
        self.session._tmp_tables.append(tbl)
        self.session._cache_registry[cache_key] = tbl
        df = IrisDataFrame(session=self.session, table_name=tbl)
        df._append_lineage("cache", f"materialized as {tbl}")
        return df

    def persist(self, storageLevel: Any = None) -> IrisDataFrame:
        return self.cache()

    def unpersist(self) -> IrisDataFrame:
        import hashlib

        sql = self.to_sql()
        cache_key = hashlib.sha256(sql.encode()).hexdigest()[:16]
        if cache_key in self.session._cache_registry:
            tbl = self.session._cache_registry.pop(cache_key)
            try:
                self.session.sql(f"DROP TABLE {tbl}")
            except Exception:
                pass
            if tbl in self.session._tmp_tables:
                self.session._tmp_tables.remove(tbl)
        return self

    def distinct(self) -> IrisDataFrame:
        df = self._copy(distinct=True)
        df._append_lineage("distinct", "")
        return df

    def drop(self, *cols: str) -> IrisDataFrame:
        df = self._copy(dropped_cols=list(cols))
        df._append_lineage("drop", ", ".join(cols))
        return df

    def union(self, other: IrisDataFrame) -> IrisDataFrame:
        parts = list(self._union_parts) if self._union_parts else [self]
        parts.append(other)
        df = self._copy(union_parts=parts)
        df._append_lineage("union", "")
        return df

    def unionAll(self, other: IrisDataFrame) -> IrisDataFrame:
        return self.union(other)

    def withColumnRenamed(self, existing: str, new: str) -> IrisDataFrame:
        renamed = dict(self._renamed_cols)
        renamed[existing] = new
        # Ensure schema is loaded before copying so _copy() can use it
        self._ensure_schema()
        df = self._copy(renamed_cols=renamed)
        df._append_lineage("withColumnRenamed", f"{existing} -> {new}")
        return df

    def dropDuplicates(self, subset: list[str] | None = None) -> IrisDataFrame:
        if subset is None:
            df = self._copy(distinct=True)
            df._append_lineage("dropDuplicates", "all columns")
            return df
        df = self._copy(drop_duplicates_subset=list(subset))
        df._append_lineage("dropDuplicates", ", ".join(subset))
        return df

    def drop_duplicates(self, subset: list[str] | None = None) -> IrisDataFrame:
        return self.dropDuplicates(subset)

    def dropna(self, how: str = "any", thresh: int | None = None, subset: list[str] | None = None) -> IrisDataFrame:
        self._ensure_schema()
        cols = subset or [c for c, _ in self._schema]
        if thresh is not None:
            conditions = " + ".join(
                f"CASE WHEN {c} IS NOT NULL THEN 1 ELSE 0 END" for c in cols
            )
            df = self._copy(filters=self.filters + [f"({conditions}) >= {thresh}"])
            df._append_lineage("dropna", f"thresh={thresh}, cols={cols}")
            return df
        if how == "all":
            conditions = " OR ".join(f"{c} IS NOT NULL" for c in cols)
            df = self._copy(filters=self.filters + [f"({conditions})"])
            df._append_lineage("dropna", f"how=all, cols={cols}")
            return df
        conditions = " AND ".join(f"{c} IS NOT NULL" for c in cols)
        df = self._copy(filters=self.filters + [conditions])
        df._append_lineage("dropna", f"how=any, cols={cols}")
        return df

    def fillna(self, value: Any, subset: list[str] | None = None) -> IrisDataFrame:
        self._ensure_schema()
        if subset is None:
            # PySpark semantics: fill only type-compatible columns when no
            # subset is given (a string fills string columns, a number fills
            # numeric columns). Avoids COALESCE(int_col, 'string') mismatch.
            cols = [
                c for c, t in (self._schema or []) if _fillna_type_compatible(value, t)
            ]
        else:
            cols = subset
        fillna = dict(self._fillna_values)
        for c in cols:
            fillna[c] = value
        df = self._copy(fillna_values=fillna)
        df._append_lineage("fillna", f"value={value}, cols={cols}")
        return df

    def na_drop(self, subset: list[str] | None = None) -> IrisDataFrame:
        return self.dropna(subset=subset)

    def na_fill(self, value: Any, subset: list[str] | None = None) -> IrisDataFrame:
        return self.fillna(value, subset)

    def na_replace(self, to_replace: Any, value: Any = None, subset: list[str] | None = None) -> IrisDataFrame:
        """Replace values matching ``to_replace`` with ``value`` (PySpark semantics).

        Supported forms: scalar→scalar (``na_replace(200.0, 999.0)``), mapping
        (``na_replace({200.0: 999.0})``), and parallel lists
        (``na_replace(['a', 'b'], ['x', 'y'])``). When ``subset`` is omitted,
        pairs are applied only to columns whose type is compatible with the
        replacement value (same rule as ``fillna``).
        """
        self._ensure_schema()
        pairs = _normalize_replace_pairs(to_replace, value)
        if subset is None:
            candidates = [c for c, _ in (self._schema or [])]
        else:
            candidates = list(subset)
        scoped: dict[str, list[tuple[Any, Any]]] = {}
        for c in candidates:
            col_type = next(
                (t for cc, t in (self._schema or []) if str(cc).lower() == str(c).lower()),
                None,
            )
            if col_type is None:
                continue
            applicable = [
                p for p in pairs if _fillna_type_compatible(p[1], col_type)
            ]
            if applicable:
                scoped[c] = applicable
        df = self._copy(replace_values=scoped)
        df._append_lineage("na_replace", f"pairs={pairs}, cols={sorted(scoped)}")
        return df

    @property
    def na(self) -> NaFunctions:
        return NaFunctions(self)

    @property
    def stat(self) -> StatFunctions:
        return StatFunctions(self)

    def explain(self, extended: bool = False) -> None:
        sql = self.to_sql(pretty=True)
        print(f"[Logical Plan]\n{sql}\n")
        self.lineage(show=True)

        # Execution Mapping
        mapping = self._execution_mapping()
        if mapping:
            print("[Execution Mapping]")
            max_label = max(len(label) for label, _ in mapping)
            for label, engine in mapping:
                print(f"  {label.ljust(max_label)} → {engine}")
            print()

        # Source
        source_lines = self._source_info()
        print("[Source]")
        for line in source_lines:
            print(f"  {line}")
        print()

        # Fallback
        os_fb, py_fb = self._fallback_summary()
        print("[Fallback]")
        print(f"  ObjectScript: {os_fb}")
        print(f"  Python:       {py_fb}")
        print()

        # Pushdown
        pct = self._pushdown_pct()
        print("[Pushdown]")
        print(f"  {pct}%")
        print()

        # Classification (§44 dimensions)
        print("[Classification]")
        print("  Execution Engine:   IRIS SQL")
        storage = self._source_info()[1].split(": ")[1] if len(self._source_info()) > 1 else "UNKNOWN"
        print(f"  Storage:            {storage}")
        print(f"  Pushdown:           {self._pushdown_pct()}%")
        parallel = "UNKNOWN"
        if "%PARALLEL" in sql.upper():
            parallel = "YES"
        print(f"  Parallel:           {parallel}")
        print("  Vectorized:         UNKNOWN")
        os_fb, py_fb = self._fallback_summary()
        print(f"  ObjectScript fb:    {os_fb}")
        print(f"  Python fb:          {py_fb}")
        print()

        if extended:
            from . import registry
            used = self._collect_functions()
            if used:
                print("[Function Registry]")
                for name in sorted(used):
                    fn = registry.get_function(name)
                    if fn:
                        print(f"  {name}: {fn.status} | {fn.compatibility} | {fn.execution}")
                    else:
                        print(f"  {name}: (unregistered)")
                print()

        try:
            plan_rows, _ = self.session.sql(f"EXPLAIN {sql}")
            print("[IRIS Explain Plan]")
            for row in plan_rows:
                print(row)
        except Exception:
            pass

    def withColumns(self, specs: dict[str, str]) -> IrisDataFrame:
        """Add or replace multiple columns at once (PySpark withColumns equivalent).

        specs: dict mapping column name to SQL expression
        e.g., df.withColumns({"total": "preco * quantidade", "discount": "preco * 0.1"})
        """
        from .functions import expr

        df = self
        for col_name, sql_expr in specs.items():
            df = df.withColumn(col_name, expr(sql_expr))
        return df

    def withColumnsRenamed(self, specs: dict[str, str]) -> IrisDataFrame:
        """Add or replace columns with renamed results."""
        df = self
        for old_name, new_name in specs.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df

    def unionByName(self, other: IrisDataFrame, allowMissingColumns: bool = False) -> IrisDataFrame:
        """Union of DataFrames by column name (not position).

        allowMissingColumns: if True, columns present in one DF but not the other
        are filled with nulls. Result column order: left columns, then
        right-only columns (PySpark-compatible).
        """
        from irispark.dataframe import IrisDataFrame as IrisFrame

        from .functions import lit

        if not isinstance(other, IrisFrame):
            raise ValueError("unionByName requires another IrisDataFrame")

        left_cols = self.columns
        right_cols = other.columns
        if left_cols == right_cols:
            return self.union(other)
        if set(left_cols) == set(right_cols):
            return self.union(other.select(*left_cols))
        if not allowMissingColumns:
            raise ValueError(
                "unionByName: column mismatch "
                f"(left-only: {sorted(set(left_cols) - set(right_cols))}, "
                f"right-only: {sorted(set(right_cols) - set(left_cols))})"
            )
        union_order = list(left_cols) + [c for c in right_cols if c not in left_cols]
        left_sel = [c if c in left_cols else lit(None).alias(c) for c in union_order]
        right_sel = [c if c in right_cols else lit(None).alias(c) for c in union_order]
        return self.select(*left_sel).union(other.select(*right_sel)).select(*union_order)

    def transform(self, func) -> IrisDataFrame:
        """Apply a transformation function to the DataFrame.

        func: callable that takes an IrisDataFrame and returns an IrisDataFrame
        """
        if not callable(func):
            raise ValueError("func must be callable")
        return func(self)

    def toDF(self) -> IrisDataFrame:
        """PySpark compatibility: return the DataFrame itself for method chaining."""
        return self

    def colRegex(self, pattern: str) -> list[str]:
        """Return column names matching a regex pattern."""
        import re
        return [c for c in self.columns if re.search(pattern, c)]

    def lineage(self, show: bool = False) -> list[dict[str, str]]:
        if show:
            print("[Lineage]")
            for i, entry in enumerate(self._lineage_entries):
                op = entry["op"]
                desc = entry.get("desc", "")
                prefix = "  └─" if i > 0 else "  ──"
                print(f"{prefix} {op}: {desc}")
            print()
        return list(self._lineage_entries)

    def _collect_functions(self) -> set[str]:
        """Scan the current SQL expression for registered function names."""
        import re

        from . import registry
        sql = self.to_sql(pretty=False)
        found: set[str] = set()
        for name in registry.FUNCTIONS:
            # Bare function call
            if re.search(rf'\b{re.escape(name)}\s*\(', sql, re.IGNORECASE):
                found.add(name)
            # UDC-prefixed form (irispark_udc_conv, irispark_udc_format_string, etc.)
            if re.search(rf'irispark_udc_{re.escape(name)}\s*\(', sql, re.IGNORECASE):
                found.add(name)
            # Schema-qualified UDAF form (IRISPARK.SKEWNESS, IRISPARK.CORR, etc.)
            if re.search(rf'IRISPARK\.{re.escape(name)}\s*\(', sql, re.IGNORECASE):
                found.add(name)
        return found

    def _append_lineage(self, op: str, desc: str = "") -> None:
        self._lineage_entries.append({"op": op, "desc": desc})

    # ── §41 explain helpers ──────────────────────────────────────────────────

    def _execution_mapping(self) -> list[tuple[str, str]]:
        """Map each lineage op to its execution engine (§41)."""
        from . import registry as _reg

        mapping: list[tuple[str, str]] = []
        used = self._collect_functions()

        # Pre-classify every registered function in the SQL by its execution type
        has_objectscript = False
        has_embedded_python = False
        has_python_fallback = False
        for name in used:
            fn = _reg.get_function(name)
            if not fn:
                continue
            if fn.execution == "objectscript":
                has_objectscript = True
            elif fn.execution == "embedded_python":
                has_embedded_python = True
            elif fn.execution == "python_fallback":
                has_python_fallback = True

        # Map each lineage entry
        for entry in self._lineage_entries:
            op = entry.get("op", "")
            desc = entry.get("desc", "")
            if op in ("source", "filter", "join", "crossJoin", "orderBy",
                       "limit", "distinct", "union", "dropna", "fillna",
                       "unpivot", "explode", "sample", "cache", "drop",
                       "withColumnRenamed", "alias"):
                mapping.append((op, "Native IRIS SQL"))
            elif op == "coalesce":
                mapping.append((op, "Client-side hint (no-op)"))
            elif op == "repartition":
                mapping.append((op, "Client-side hint (no-op)"))
            elif op in ("select", "selectExpr", "withColumn"):
                # Determine if any function in this op uses a special engine
                label = f"{op}: {desc[:40]}..." if len(desc) > 40 else f"{op}: {desc}"
                if has_python_fallback:
                    mapping.append((label, "Python fallback"))
                elif has_embedded_python:
                    mapping.append((label, "Embedded Python"))
                elif has_objectscript:
                    mapping.append((label, "ObjectScript UDAF"))
                else:
                    mapping.append((label, "Native IRIS SQL"))
            elif op == "agg":
                label = f"{op}: {desc[:40]}..." if len(desc) > 40 else f"{op}: {desc}"
                if has_python_fallback:
                    mapping.append((label, "Python fallback"))
                elif has_objectscript:
                    mapping.append((label, "ObjectScript UDAF"))
                elif has_embedded_python:
                    mapping.append((label, "Embedded Python"))
                else:
                    mapping.append((label, "Native IRIS SQL"))
            else:
                mapping.append((op, "Native IRIS SQL"))
        return mapping

    def _source_info(self) -> list[str]:
        """Return source table and storage mode (best-effort, §41)."""
        lines: list[str] = []
        table = self.table_name
        lines.append(f"Table: {table}")
        storage = "UNKNOWN"
        try:
            from .iris_extensions import _split_table_name
            schema, tbl = _split_table_name(table)
            # Best-effort columnar detection via index name suffix
            where_parts = ["INDEX_NAME LIKE '%_col'"]
            if tbl:
                where_parts.append(f"TABLE_NAME = '{tbl}'")
            if schema:
                where_parts.append(f"TABLE_SCHEMA = '{schema}'")
            sql = (
                "SELECT 1 FROM INFORMATION_SCHEMA.INDEXES "
                f"WHERE {' AND '.join(where_parts)}"
            )
            rows, _ = self.session.sql(sql)
            if rows:
                storage = "COLUMNAR"
            else:
                storage = "ROW"
        except Exception:
            pass
        lines.append(f"Storage: {storage}")
        return lines

    def _fallback_summary(self) -> tuple[str, str]:
        """Return (ObjectScript fallback, Python fallback) as YES/NO (§41)."""
        used = self._collect_functions()
        os_fb = "NO"
        py_fb = "NO"
        from . import registry as _reg
        for name in used:
            fn = _reg.get_function(name)
            if not fn:
                continue
            if fn.execution == "objectscript":
                os_fb = "YES"
            if fn.execution in ("embedded_python", "python_fallback"):
                py_fb = "YES"
        return os_fb, py_fb

    def _pushdown_pct(self) -> str:
        """Compute pushdown percentage (§41)."""
        mapping = self._execution_mapping()
        if not mapping:
            return "100"
        total = len(mapping)
        native = sum(
            1
            for _, engine in mapping
            if engine in ("Native IRIS SQL", "Client-side hint (no-op)")
        )
        pct = int((native / total) * 100)
        return str(pct)

    # ── end §41 explain helpers ────────────────────────────────────────────────

    def _ensure_schema(self) -> list[tuple[str, Any]]:
        schema = getattr(self, "_schema", None)
        if schema:
            return schema
        _validate_identifier("table_name", self.table_name)
        cursor = self.session.conn.cursor()

        if self.join_config:
            cursor.execute(f"SELECT * FROM {self.table_name} LIMIT 0")
            left_cols = [(d[0], d[1]) for d in cursor.description]
            cursor.close()

            left_names: dict[str, int] = {}
            merged: list[tuple[str, Any]] = []
            for c, t in left_cols:
                left_names[c.lower()] = left_names.get(c.lower(), 0) + 1
                merged.append((c, t))

            self._right_suffix: dict[str, str] = {}
            self._left_schema = left_cols
            self._right_schema: list[tuple[str, list[tuple[str, Any]]]] = []

            multi = len(self.join_config) > 1
            for i, link in enumerate(self.join_config):
                right_df = link["right"]
                right_schema = right_df._ensure_schema()
                alias = getattr(right_df, '_alias', None) or (f"r{i}" if multi else "r")

                right_cols_suffixed: list[tuple[str, Any]] = []
                for c, t in right_schema:
                    if c.lower() in left_names:
                        suffix = left_names[c.lower()]
                        left_names[c.lower()] += 1
                        suf = f"{c}_{suffix}"
                        self._right_suffix[suf] = c
                        right_cols_suffixed.append((suf, t))
                    else:
                        right_cols_suffixed.append((c, t))
                        left_names[c.lower()] = left_names.get(c.lower(), 0) + 1

                merged.extend(right_cols_suffixed)
                self._right_schema.append((alias, right_cols_suffixed))

            self._schema = merged
            if self._base_schema is None:
                self._base_schema = list(merged)
            return self._schema

        cursor.execute(f"SELECT * FROM {self.table_name} LIMIT 0")
        columns = [d[0] for d in cursor.description]
        type_codes = [d[1] for d in cursor.description]
        self._schema = [(c, t) for c, t in zip(columns, type_codes)]
        if self._base_schema is None:
            self._base_schema = list(self._schema)
        return self._schema

    @property
    def columns(self) -> list[str]:
        self._ensure_schema()
        cols = [c for c, _ in self._schema]
        if self._renamed_cols:
            cols = [self._renamed_cols.get(c, c) for c in cols]
        for col_name, _ in self.with_columns:
            if col_name not in cols:
                cols.append(col_name)
        return cols

    @property
    def dtypes(self) -> list[tuple[str, str]]:
        self._ensure_schema()
        return [(c, _type_code_to_name(t)) for c, t in self._schema]

    @property
    def schema(self) -> StructType:
        from .types import StructField, StructType
        self._ensure_schema()
        fields: list[StructField] = []
        for c, t in self._schema:
            dt = _type_code_to_datatype(t)
            fields.append(StructField(c, dt))
        return StructType(fields)

    def printSchema(self) -> None:
        self._ensure_schema()
        print("root")
        for i, (c, t) in enumerate(self._schema):
            dt = _type_code_to_name(t)
            print(f" |-- {c}: {dt} (nullable = true)")

    def createOrReplaceTempView(self, name: str) -> None:
        self.session._register_temp_view(name, self)

    def createTempView(self, name: str) -> None:
        self.session._register_temp_view(name, self)

    def registerTempTable(self, name: str) -> None:
        self.createOrReplaceTempView(name)

    def to_sql(self, pretty: bool = False) -> str:
        sql = SQLGenerator(self).generate()
        if pretty:
            from .sql_generator import _format_sql
            return _format_sql(sql)
        return sql

    @property
    def iris(self) -> IrisExtensions:
        """Access IRIS-specific extensions.

        Returns:
            IrisExtensions: Object providing IRIS-specific functionality
            like columnar indexes, bitmap indexes, and table statistics.
        """
        from .iris_extensions import IrisExtensions
        return IrisExtensions(self)

    def collect(self) -> list[Row]:
        rows, columns = self.session.sql(self.to_sql())
        col_names = [str(c) for c in columns]
        from .row import _make_row
        return [_make_row(row, col_names) for row in rows]

    def to_arrow(self) -> pa.RecordBatch:
        rows, columns = self.session.sql(self.to_sql())
        col_names = [str(c) for c in columns]
        schema_map = dict(self._ensure_schema() or [])
        if not rows:
            types = [pa.bool_() if schema_map.get(n) == 16 else pa.null() for n in col_names]
            return pa.RecordBatch.from_arrays(
                [pa.array([], type=t) for t in types],
                names=col_names,
            )
        col_data = list(zip(*rows))
        arrays = []
        for i, name in enumerate(col_names):
            data = col_data[i]
            if schema_map.get(name) == 16:
                arrays.append(pa.array(data, type=pa.bool_()))
            else:
                arrays.append(pa.array(data))
        return pa.RecordBatch.from_arrays(arrays, names=col_names)

    def to_pandas(self, warn_threshold: int = 100_000) -> Any:
        """Convert to pandas DataFrame with optional large-dataset guardrail."""
        count = self.count()
        if count > warn_threshold:
            warnings.warn(
                f"DataFrame has {count} rows, exceeding warn_threshold={warn_threshold}. "
                "Consider using distributed processing or sampling.",
                UserWarning,
                stacklevel=2,
            )
        return self.to_arrow().to_pandas()

    toPandas = to_pandas

    def to_polars(self) -> Any:
        import polars as pl
        return pl.from_arrow(pa.Table.from_batches([self.to_arrow()]))

    def show(self, n: int = 10) -> None:
        if self.limit_n is not None:
            n = min(n, self.limit_n)
        print(self.limit(n).to_pandas())

    def _copy(self, **kwargs: Any) -> IrisDataFrame:
        params: dict[str, Any] = {
            "session": self.session,
            "table_name": self.table_name,
            "select_cols": list(self.select_cols),
            "filters": list(self.filters),
            "group_cols": list(self.group_cols),
            "aggregations": dict(self.aggregations),
            "limit_n": self.limit_n,
            "order_cols": list(self.order_cols),
            "with_columns": list(self.with_columns),
            "join_config": [dict(j) for j in self.join_config],
            "distinct": self._distinct,
            "dropped_cols": list(self._dropped_cols),
            "union_parts": list(self._union_parts),
            "renamed_cols": dict(self._renamed_cols),
            "fillna_values": dict(self._fillna_values),
            "replace_values": {c: list(ps) for c, ps in self._replace_values.items()},
            "grouped_base_columns": list(self._grouped_base_columns),
            "lineage_entries": list(self._lineage_entries),
            "unpivot_config": self._unpivot_config,
        }
        if hasattr(self, "_drop_duplicates_subset"):
            params["drop_duplicates_subset"] = list(self._drop_duplicates_subset)
        params["sample_fraction"] = self._sample_fraction
        params["sample_seed"] = self._sample_seed
        params["sample_with_replacement"] = self._sample_with_replacement
        params["random_split"] = self._random_split
        params["agg_exprs"] = list(self._agg_exprs) if self._agg_exprs else []
        params["pivot_col"] = self._pivot_col
        params["pivot_values"] = list(self._pivot_values) if self._pivot_values else None
        params["group_type"] = self._group_type
        params.update(kwargs)
        df = IrisDataFrame(**params)
        schema = None
        if kwargs.get("join_config") or kwargs.get("union_parts"):
            pass
        else:
            schema = getattr(self, "_schema", None)
            if not schema and not self.join_config and not self._union_parts:
                try:
                    schema = self._ensure_schema()
                except Exception:
                    schema = None
            if schema is not None:
                if "select_cols" in kwargs and kwargs["select_cols"] != self.select_cols:
                    cols_arg = kwargs["select_cols"]
                    if len(cols_arg) == 1 and isinstance(cols_arg[0], str) and cols_arg[0] == "*":
                        df._schema = list(schema)
                    else:
                        schema_map = {c.lower(): (c, t) for c, t in schema}
                        # Build schema with PHYSICAL column names, not aliases
                        # Resolve aliases back to physical names using reversed_renames
                        # Use self._renamed_cols (from params) not kwargs.get("renamed_cols")
                        reversed_renames = {v: k for k, v in self._renamed_cols.items()}
                        df._schema = []
                        for c in cols_arg:
                            if isinstance(c, str):
                                # Check if c is an alias, resolve to physical name
                                physical = reversed_renames.get(c, c)
                                # Look up in schema_map using physical name
                                if physical.lower() in schema_map:
                                    # Store PHYSICAL name, not alias
                                    df._schema.append(schema_map[physical.lower()])
                                elif c.lower() in schema_map:
                                    # c is not an alias, use it directly
                                    df._schema.append(schema_map[c.lower()])
                                else:
                                    df._schema.append((str(c), "VARCHAR(4000)"))
                            else:
                                df._schema.append((str(c), "VARCHAR(4000)"))
                elif "dropped_cols" in kwargs and kwargs["dropped_cols"]:
                    dropped = {c.lower() for c in kwargs["dropped_cols"]}
                    df._schema = [(c, t) for c, t in schema if c.lower() not in dropped]
                elif "renamed_cols" in kwargs and kwargs["renamed_cols"] != self._renamed_cols:
                    # renamed_cols changed - propagate schema so _simple_table_source() can use it
                    df._schema = list(schema)
                else:
                    df._schema = list(schema)
        # If this copy obtained a schema but the parent has no _base_schema yet,
        # lock the full schema as the base schema so chained projections can still
        # look up types for columns that are not in the projected _schema.
        if schema is not None and self._base_schema is None:
            self._base_schema = list(schema)
        base_schema = getattr(self, "_base_schema", None)
        if base_schema is not None:
            df._base_schema = list(base_schema)
        for attr in ("_left_schema", "_right_schema", "_right_suffix"):
            val = getattr(self, attr, None)
            if val is not None:
                setattr(df, attr, val if isinstance(val, dict) else list(val))
        if self._alias is not None and "table_name" not in kwargs:
            df._alias = self._alias
        return df

class NaFunctions:
    """Na (missing data) functions namespace for PySpark compatibility."""

    def __init__(self, df: IrisDataFrame):
        self._df = df

    def drop(self, subset: list[str] | None = None) -> IrisDataFrame:
        """Drop rows with null values."""
        return self._df.dropna(subset=subset)

    def fill(self, value: Any, subset: list[str] | None = None) -> IrisDataFrame:
        """Fill null values with a specified value."""
        return self._df.fillna(value, subset)

    def replace(self, to_replace: Any, value: Any = None, subset: list[str] | None = None) -> IrisDataFrame:
        """Replace values matching ``to_replace`` with ``value`` (PySpark semantics)."""
        return self._df.na_replace(to_replace, value, subset)

class StatFunctions:
    """Stat functions namespace for PySpark compatibility."""

    def __init__(self, df: IrisDataFrame):
        self._df = df

    def corr(self, col1: str, col2: str | None = None) -> Any:
        """Compute correlation of two columns."""
        from irispark.functions import corr as _corr_fn
        target = col1 if col2 is None else col2
        pdf = self._df.select(_corr_fn(col1, target)).to_pandas()
        if pdf.empty or pdf.iloc[0, 0] is None:
            return 1.0 if col2 is None else None
        return pdf.iloc[0, 0]

    def cov(self, col1: str, col2: str) -> Any:
        """Compute sample covariance of two columns.

        Computed inside IRIS via the expanded :func:`covar_samp` SQL formula;
        only the single scalar result is fetched. ``None`` is returned when no
        valid observation pair exists (PySpark returns ``0.0`` there — noted
        deviation, consistent with :meth:`corr`).
        """
        from irispark.functions import covar_samp as _cov_fn
        pdf = self._df.select(_cov_fn(col1, col2)).to_pandas()
        if pdf.empty or pdf.iloc[0, 0] is None:
            return None
        return pdf.iloc[0, 0]

    def crosstab(self, row: str, col: str) -> Any:
        """Compute crosstabulation of two columns."""
        import pandas as pd

        from irispark.functions import col as _col
        pdf = self._df.select(
            _col(row).alias("row_col"), _col(col).alias("col_val")
        ).to_pandas()
        if pdf.empty:
            return {}
        return pd.crosstab(pdf["row_col"], pdf["col_val"])

    def freqItems(self, support: float | None = None) -> Any:
        """Find frequent items in the data."""
        pdf = self._df.select("*").to_pandas()
        if pdf.empty:
            return {}
        first_col = pdf.columns[0]
        counts = pdf[first_col].value_counts()
        if support is not None:
            return counts[counts >= support].to_dict()
        return counts.to_dict()

    def sampleBy(self, col: str, fractions: dict | None = None, sampleByColumns: list[str] | None = None) -> Any:
        """Stratified sampling."""
        import pandas as pd
        pdf = self._df.select(col).to_pandas()
        if pdf.empty:
            return pd.DataFrame()
        if fractions is None:
            fractions = {col: 0.5}
        len(pdf)
        sampled = pdf.sample(frac=fractions.get(col, 0.5), random_state=42)
        return sampled
