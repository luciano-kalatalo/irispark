from __future__ import annotations

import sys
from copy import copy
from typing import Any

from .column import Column

_currentRow = 0
_unboundedPreceding = -sys.maxsize
_unboundedFollowing = sys.maxsize


class Window:
    currentRow = _currentRow
    unboundedPreceding = _unboundedPreceding
    unboundedFollowing = _unboundedFollowing

    @staticmethod
    def partitionBy(*cols: Any) -> WindowSpec:
        return WindowSpec().partitionBy(*cols)

    @staticmethod
    def orderBy(*cols: Any) -> WindowSpec:
        return WindowSpec().orderBy(*cols)

    @staticmethod
    def rowsBetween(start: int, end: int) -> WindowSpec:
        return WindowSpec().rowsBetween(start, end)

    @staticmethod
    def rangeBetween(start: int, end: int) -> WindowSpec:
        return WindowSpec().rangeBetween(start, end)


class WindowSpec:
    def __init__(self) -> None:
        self._partition_cols: list[Any] = []
        self._order_cols: list[Any] = []
        self._frame: tuple[str, int, int] | None = None

    def partitionBy(self, *cols: Any) -> WindowSpec:
        new = copy(self)
        new._partition_cols = list(cols)
        return new

    def orderBy(self, *cols: Any) -> WindowSpec:
        new = copy(self)
        new._order_cols = list(cols)
        return new

    def rowsBetween(self, start: int, end: int) -> WindowSpec:
        new = copy(self)
        new._frame = ("ROWS", start, end)
        return new

    def rangeBetween(self, start: int, end: int) -> WindowSpec:
        new = copy(self)
        new._frame = ("RANGE", start, end)
        return new

    def _serialize_col(self, col: Any) -> str:
        if isinstance(col, Column):
            return col._expr
        if isinstance(col, str):
            return col
        return str(col)

    def _frame_bound(self, bound: int) -> str:
        if bound == _unboundedPreceding:
            return "UNBOUNDED PRECEDING"
        if bound == _unboundedFollowing:
            return "UNBOUNDED FOLLOWING"
        if bound == _currentRow:
            return "CURRENT ROW"
        if bound < 0:
            return f"{abs(bound)} PRECEDING"
        return f"{bound} FOLLOWING"

    def _to_sql(self) -> str:
        parts: list[str] = []
        if self._partition_cols:
            cols = ", ".join(self._serialize_col(c) for c in self._partition_cols)
            parts.append(f"PARTITION BY {cols}")
        if self._order_cols:
            cols = ", ".join(self._serialize_col(c) for c in self._order_cols)
            parts.append(f"ORDER BY {cols}")
        if self._frame:
            frame_type, start, end = self._frame
            parts.append(
                f"{frame_type} BETWEEN {self._frame_bound(start)} "
                f"AND {self._frame_bound(end)}"
            )
        return " ".join(parts)
