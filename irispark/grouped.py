from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .dataframe import IrisDataFrame

_TYPE_CODE_NAME = {4: "INT", 5: "DOUBLE", 6: "FLOAT", 7: "DECIMAL",
                   2: "VARCHAR", 12: "DATE", 11: "TIMESTAMP", -1: "VARCHAR"}


class GroupedData:
    def __init__(self, dataframe: IrisDataFrame) -> None:
        self._df = dataframe

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._df, name)

    def pivot(self, pivot_col: str, values: list[Any] | None = None) -> GroupedData:
        """Pivot a column and aggregate per value (PySpark-compatible).

        When ``values`` is omitted, the distinct values of ``pivot_col`` are
        discovered first via an eager scan (mirroring PySpark's behavior when
        ``values`` is not provided; those discovered values become the pivot
        output columns).
        """
        if values is None:
            base = self._df._copy(group_cols=[], pivot_col=None, pivot_values=None)
            rows = base.select(pivot_col).distinct().collect()
            values = [row[0] for row in rows]
        if not values:
            raise ValueError("pivot values list must not be empty")
        df = self._df._copy(pivot_col=pivot_col, pivot_values=values)
        return GroupedData(df)

    def agg(self, *exprs: Any) -> IrisDataFrame:
        if not exprs:
            raise ValueError(
                "agg() requires at least one column expression or an aggregate dict"
            )
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            return self._df.agg(exprs[0])
        return self._df.agg(*list(exprs))

    def _resolve_cols(self, specified: list[str]) -> list[str]:
        if specified:
            return list(specified)
        group_set = set(str(g) for g in self._df.group_cols)
        schema = getattr(self._df, "_schema", None) or []
        num_types = {"INT", "DOUBLE", "FLOAT", "DECIMAL", "NUMERIC"}
        num_cols: list[str] = []
        for c, t in schema:
            t_str = str(t).upper() if not isinstance(t, int) else _TYPE_CODE_NAME.get(t, "")
            if t_str in num_types and c not in group_set:
                num_cols.append(c)
        return num_cols

    def avg(self, *cols: str) -> IrisDataFrame:
        resolved = self._resolve_cols(list(cols))
        aggs = {c: "avg" for c in resolved}
        return self._df.agg(aggs)

    def mean(self, *cols: str) -> IrisDataFrame:
        return self.avg(*cols)

    def sum(self, *cols: str) -> IrisDataFrame:
        resolved = self._resolve_cols(list(cols))
        aggs = {c: "sum" for c in resolved}
        return self._df.agg(aggs)

    def count(self) -> IrisDataFrame:
        return self._df.agg({"*": "count"})

    def min(self, *cols: str) -> IrisDataFrame:
        resolved = self._resolve_cols(list(cols))
        aggs = {c: "min" for c in resolved}
        return self._df.agg(aggs)

    def max(self, *cols: str) -> IrisDataFrame:
        resolved = self._resolve_cols(list(cols))
        aggs = {c: "max" for c in resolved}
        return self._df.agg(aggs)
