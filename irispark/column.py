from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .types import DataType

if TYPE_CHECKING:
    from .functions import CaseWhen


class Column:
    def __init__(self, expr: str) -> None:
        self._expr = expr

    def _col(self, other: Any) -> Column:
        if isinstance(other, Column):
            return other
        return Column(_quote(other))

    def __lt__(self, other: Any) -> Column:
        return Column(f"({self._expr} < {self._col(other)._expr})")

    def __le__(self, other: Any) -> Column:
        return Column(f"({self._expr} <= {self._col(other)._expr})")

    def __gt__(self, other: Any) -> Column:
        return Column(f"({self._expr} > {self._col(other)._expr})")

    def __ge__(self, other: Any) -> Column:
        return Column(f"({self._expr} >= {self._col(other)._expr})")

    def __eq__(self, other: Any) -> Column:  # type: ignore[override]  # DSL: == builds a Column, not bool (PySpark parity)
        if other is None:
            return Column(f"({self._expr} IS NULL)")
        if isinstance(other, str):
            return Column(f"(%EXACT {self._expr} = {self._col(other)._expr})")
        return Column(f"({self._expr} = {self._col(other)._expr})")

    def __ne__(self, other: Any) -> Column:  # type: ignore[override]  # DSL override (PySpark parity)
        if other is None:
            return Column(f"({self._expr} IS NOT NULL)")
        if isinstance(other, str):
            return Column(f"(%EXACT {self._expr} <> {self._col(other)._expr})")
        return Column(f"({self._expr} <> {self._col(other)._expr})")

    def __and__(self, other: Any) -> Column:
        return Column(f"({self._expr} AND {self._col(other)._expr})")

    def __or__(self, other: Any) -> Column:
        return Column(f"({self._expr} OR {self._col(other)._expr})")

    def __invert__(self) -> Column:
        return Column(f"(NOT {self._expr})")

    def __add__(self, other: Any) -> Column:
        return Column(f"({self._expr} + {self._col(other)._expr})")

    def __radd__(self, other: Any) -> Column:
        return Column(f"({self._col(other)._expr} + {self._expr})")

    def __sub__(self, other: Any) -> Column:
        return Column(f"({self._expr} - {self._col(other)._expr})")

    def __rsub__(self, other: Any) -> Column:
        return Column(f"({self._col(other)._expr} - {self._expr})")

    def __mul__(self, other: Any) -> Column:
        return Column(f"({self._expr} * {self._col(other)._expr})")

    def __rmul__(self, other: Any) -> Column:
        return Column(f"({self._col(other)._expr} * {self._expr})")

    def __truediv__(self, other: Any) -> Column:
        return Column(f"({self._expr} / {self._col(other)._expr})")

    def __mod__(self, other: Any) -> Column:
        return Column(f"MOD({self._expr}, {self._col(other)._expr})")

    def __hash__(self) -> int:
        return hash(self._expr)

    def between(self, lowerBound: Any, upperBound: Any) -> Column:
        return Column(
            f"({self._expr} BETWEEN {_quote(lowerBound)} AND {_quote(upperBound)})"
        )

    def isNull(self) -> Column:
        """PySpark-compatible null check; scalar in SELECT, predicate in WHERE."""
        return PredicateColumn(
            f"(CASE WHEN {self._expr} IS NULL THEN 1 ELSE 0 END)",
            f"({self._expr} IS NULL)",
        )

    def isNotNull(self) -> Column:
        """PySpark-compatible non-null check; scalar in SELECT, predicate in WHERE."""
        return PredicateColumn(
            f"(CASE WHEN {self._expr} IS NOT NULL THEN 1 ELSE 0 END)",
            f"({self._expr} IS NOT NULL)",
        )

    def cast(self, dataType: str | DataType) -> Column:
        if isinstance(dataType, DataType):
            sql_type = dataType.sqlTypeName()
        else:
            sql_type = str(dataType)
        return Column(f"CAST({self._expr} AS {sql_type})")

    def astype(self, dataType: str | DataType) -> Column:
        """Alias for :meth:`cast` (PySpark-compatible)."""
        return self.cast(dataType)

    def name(self, name: str) -> Column:
        """Alias for :meth:`alias` (PySpark-compatible)."""
        return self.alias(name)

    def substr(self, startPos: int, length: int) -> Column:
        """Extract a 1-based substring (PySpark-compatible, SUBSTRING on IRIS)."""
        return Column(f"SUBSTRING({self._expr}, {_quote(startPos)}, {_quote(length)})")

    def when(self, condition: Column, value: Any) -> CaseWhen:
        """Conditional value selection; chain ``.when()``/``.otherwise()`` after it."""
        from .functions import CaseWhen
        return CaseWhen().when(condition, value)

    def isNaN(self) -> Column:
        """True when the expression is NaN (a value unequal to itself)."""
        return Column(f"({self._expr} <> {self._expr})")

    def eqNullSafe(self, other: Any) -> Column:
        """Equality that treats NULL == NULL as true (PySpark-compatible)."""
        other_expr = self._col(other)._expr
        return Column(
            f"(({self._expr} = {other_expr}) OR ({self._expr} IS NULL AND {other_expr} IS NULL))"
        )

    def ilike(self, pattern: str) -> Column:
        """Case-insensitive LIKE (PySpark-compatible)."""
        return Column(f"(UPPER({self._expr}) LIKE UPPER({_quote(pattern)}))")

    def asc_nulls_first(self) -> Column:
        """Sort ascending with NULLs first (PySpark-compatible)."""
        return SortColumn(
            f"CASE WHEN {self._expr} IS NULL THEN 1 ELSE 0 END DESC, {self._expr} ASC",
            f"CASE WHEN {self._expr} IS NULL THEN 1 ELSE 0 END ASC, {self._expr} DESC",
        )

    def asc_nulls_last(self) -> Column:
        """Sort ascending with NULLs last (PySpark-compatible)."""
        return SortColumn(
            f"CASE WHEN {self._expr} IS NULL THEN 1 ELSE 0 END ASC, {self._expr} ASC",
            f"CASE WHEN {self._expr} IS NULL THEN 1 ELSE 0 END DESC, {self._expr} DESC",
        )

    def desc_nulls_first(self) -> Column:
        """Sort descending with NULLs first (PySpark-compatible)."""
        return SortColumn(
            f"CASE WHEN {self._expr} IS NULL THEN 1 ELSE 0 END DESC, {self._expr} DESC",
            f"CASE WHEN {self._expr} IS NULL THEN 1 ELSE 0 END ASC, {self._expr} ASC",
        )

    def desc_nulls_last(self) -> Column:
        """Sort descending with NULLs last (PySpark-compatible)."""
        return SortColumn(
            f"CASE WHEN {self._expr} IS NULL THEN 1 ELSE 0 END ASC, {self._expr} DESC",
            f"CASE WHEN {self._expr} IS NULL THEN 1 ELSE 0 END DESC, {self._expr} ASC",
        )

    def isin(self, values) -> Column:
        vals = ", ".join(_quote(v) for v in values)
        return Column(f"({self._expr} IN ({vals}))")

    def asc(self) -> Column:
        return Column(f"{self._expr} ASC")

    def desc(self) -> Column:
        return Column(f"{self._expr} DESC")

    def alias(self, name: str) -> Column:
        return Column(f"{self._expr} AS {name}")

    def over(self, window_spec) -> Column:
        from .window import WindowSpec
        if not isinstance(window_spec, WindowSpec):
            raise TypeError(
                f"over() requires a WindowSpec, got {type(window_spec).__name__}"
            )
        return Column(f"{self._expr} OVER ({window_spec._to_sql()})")

    def like(self, pattern: str) -> Column:
        return Column(f"%EXACT {self._expr} LIKE {_quote(pattern)}")

    def rlike(self, pattern: str) -> Column:
        return Column(f"regexp_extract({self._expr}, {_quote(pattern)}, 0) != ''")

    def contains(self, other: Any) -> Column:
        val = _quote(other)
        return Column(f"%EXACT {self._expr} LIKE '%' || {val} || '%'")

    def startswith(self, other: Any) -> Column:
        val = _quote(other)
        return Column(f"%EXACT {self._expr} LIKE {val} || '%'")

    def endswith(self, other: Any) -> Column:
        val = _quote(other)
        return Column(f"%EXACT {self._expr} LIKE '%' || {val}")

    def __repr__(self) -> str:
        return f"Column({self._expr!r})"


class PredicateColumn(Column):
    """Column that carries both a scalar form (for SELECT) and a predicate form (for WHERE/HAVING).

    Some SQL dialects (notably IRIS) do not accept boolean predicates such as
    ``(col IS NULL)`` as scalar SELECT expressions. A ``PredicateColumn`` keeps
    the scalar form in ``_expr`` (e.g. ``CASE WHEN col IS NULL THEN 1 ELSE 0 END``)
    so ``withColumn`` and ``select`` work, while exposing a boolean
    ``_predicate`` form (e.g. ``(col IS NULL)``) for use in ``filter`` / ``WHERE``.
    """

    def __init__(self, scalar_expr: str, predicate_expr: str) -> None:
        super().__init__(scalar_expr)
        self._predicate = predicate_expr


class CoalesceColumn(Column):
    """Column carrying structured args for COALESCE/IFNULL/NVL functions.

    IRIS SQL requires exact type matching inside COALESCE/IFNULL; raw literals
    such as ``0.0`` may be inferred as VARCHAR and clash with a NUMERIC column.
    ``CoalesceColumn`` records the original argument values so the SQL generator
    can look up the anchor column's type from the DataFrame schema and cast
    numeric literal arguments to that type at render time.
    """

    def __init__(self, func_name: str, args: list[Any], expr: str) -> None:
        super().__init__(expr)
        self._coalesce_func = func_name
        self._coalesce_args = args


class SortColumn(Column):
    """Column carrying a complete ORDER BY fragment (possibly multi-key).

    IRIS has no ``NULLS FIRST/LAST``, so null-ordering is emulated with a
    ``CASE WHEN col IS NULL THEN 1 ELSE 0 END`` flag plus the real sort key,
    e.g. ``CASE WHEN x IS NULL THEN 1 ELSE 0 END DESC, x ASC``. A ``SortColumn``
    must be serialized verbatim (no reserved-word quoting of the first token),
    which is what ``sql_generator._serialize_order_col`` checks for.
    ``reverse_expr`` is the direction-mirrored fragment, used by ``tail()``.
    """

    def __init__(self, expr: str, reverse_expr: str) -> None:
        super().__init__(expr)
        self._reverse_expr = reverse_expr


def _quote(value: Any) -> str:
    if isinstance(value, Column):
        return value._expr
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, bool):
        return "1" if value else "0"
    if value is None:
        return "NULL"
    if isinstance(value, float):
        import math

        if math.isnan(value):
            return "NULL"
    return str(value)
