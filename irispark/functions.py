from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .column import CoalesceColumn, Column, PredicateColumn, _quote
from .session import get_active_session
from .types import DataType


def _to_sql_expr(arg: Any) -> str:
    if isinstance(arg, Column):
        return arg._expr
    return _quote(arg)


def _to_col_expr(arg: Any) -> str:
    """Column reference: Column -> its expression, str -> bare column name."""
    if isinstance(arg, Column):
        return arg._expr
    if isinstance(arg, str):
        return arg
    raise TypeError(f"expected column reference, got {type(arg).__name__}")


def _predicate_expr(arg: Any) -> str:
    """Boolean-context expression: strings are raw SQL, Columns use predicate form."""
    if isinstance(arg, Column):
        return getattr(arg, "_predicate", None) or arg._expr
    return f"({arg})"


class CaseWhen:
    def __init__(self) -> None:
        self._cases: list[tuple[Column, Any]] = []

    def when(self, condition: Column, value: Any) -> CaseWhen:
        self._cases.append((condition, value))
        return self

    @property
    def _expr(self) -> str:
        """SQL form with implicit ``ELSE NULL`` (PySpark semantics for a bare
        ``when`` chain). Lets aggregates accept ``count(when(...))`` directly."""
        return self._render(default=None)

    def otherwise(self, default: Any) -> Column:
        return Column(self._render(default=default))

    def _render(self, default: Any) -> str:
        sql = (
            "CASE "
            + " ".join(
                f"WHEN {getattr(c, '_predicate', None) or c._expr} THEN {_quote(v)}"
                for c, v in self._cases
            )
        )
        if default is not None:
            sql += f" ELSE {_quote(default)}"
        return sql + " END"


def col(name: str) -> Column:
    return Column(name)


def desc(col_ref: str | Column) -> Column:
    """Sort column in descending order (PySpark-compatible)."""
    if isinstance(col_ref, Column):
        col_ref = col_ref._expr
    return Column(f"{col_ref} DESC")


def asc(col_ref: str | Column) -> Column:
    """Sort column in ascending order (PySpark-compatible)."""
    if isinstance(col_ref, Column):
        col_ref = col_ref._expr
    return Column(f"{col_ref} ASC")


def when(condition: Column, value: Any) -> CaseWhen:
    return CaseWhen().when(condition, value)


def lit(value: Any) -> Column:
    return Column(_quote(value))


def expr(expression: str) -> Column:
    return Column(expression)


def broadcast(df: Any) -> Any:
    return df


def cast(col_ref: str | Column, data_type: str | DataType) -> Column:
    from .column import Column
    if isinstance(col_ref, Column):
        return col_ref.cast(data_type)
    return Column(col_ref).cast(data_type)


def floor(col_ref: str | Column) -> Column:
    return Column(f"FLOOR({_to_col_expr(col_ref)})")


def ceil(col_ref: str | Column) -> Column:
    return Column(f"CEILING({_to_col_expr(col_ref)})")


ceiling = ceil


def round(col_ref: str | Column, scale: int = 0) -> Column:
    return Column(f"ROUND({_to_col_expr(col_ref)}, {_to_sql_expr(scale)})")


def abs(col_ref: str | Column) -> Column:
    return Column(f"ABS({_to_col_expr(col_ref)})")


def sqrt(col_ref: str | Column) -> Column:
    return Column(f"SQRT({_to_col_expr(col_ref)})")


def _pow_sql(a: str, b: str) -> str:
    # IRIS raises a fatal SQL error (SQLCODE -400) where Spark returns
    # +/-Infinity or NaN: POWER(0, negative) and POWER(negative, fractional).
    # Guard both to NULL (IRIS cannot represent Inf/NaN).
    return (
        f"CASE WHEN {a} = 0 AND {b} < 0 THEN NULL "
        f"WHEN {a} < 0 AND MOD({b}, 1) <> 0 THEN NULL "
        f"ELSE POWER({a}, {b}) END"
    )


def pow(col1: str | Column, col2: str | Column) -> Column:
    return Column(_pow_sql(_to_col_expr(col1), _to_sql_expr(col2)))


def exp(col_ref: str | Column) -> Column:
    return Column(f"EXP({_to_col_expr(col_ref)})")


def log(arg1: str | Column, arg2: str | Column | None = None) -> Column:
    if arg2 is None:
        return Column(f"LOG({_to_col_expr(arg1)})")
    return Column(f"LOG({_to_col_expr(arg2)}) / LOG({_to_sql_expr(arg1)})")


def log10(col_ref: str | Column) -> Column:
    return Column(f"LOG10({_to_col_expr(col_ref)})")


def ln(col_ref: str | Column) -> Column:
    return Column(f"LOG({_to_col_expr(col_ref)})")


def sign(col_ref: str | Column) -> Column:
    return Column(f"SIGN({_to_col_expr(col_ref)})")


signum = sign


def sin(col_ref: str | Column) -> Column:
    return Column(f"SIN({_to_col_expr(col_ref)})")


def cos(col_ref: str | Column) -> Column:
    return Column(f"COS({_to_col_expr(col_ref)})")


def tan(col_ref: str | Column) -> Column:
    return Column(f"TAN({_to_col_expr(col_ref)})")


def degrees(col_ref: str | Column) -> Column:
    return Column(f"DEGREES({_to_col_expr(col_ref)})")


def radians(col_ref: str | Column) -> Column:
    return Column(f"RADIANS({_to_col_expr(col_ref)})")


def negative(col_ref: str | Column) -> Column:
    return Column(f"-({_to_col_expr(col_ref)})")


negate = negative


def positive(col_ref: str | Column) -> Column:
    return Column(f"+({_to_col_expr(col_ref)})")


def upper(col_ref: str | Column) -> Column:
    return Column(f"UPPER({_to_col_expr(col_ref)})")


def lower(col_ref: str | Column) -> Column:
    return Column(f"LOWER({_to_col_expr(col_ref)})")


def lcase(col_ref: str | Column) -> Column:
    return Column(f"LCASE({_to_col_expr(col_ref)})")


def ucase(col_ref: str | Column) -> Column:
    return Column(f"UCASE({_to_col_expr(col_ref)})")


def trim(col_ref: str | Column) -> Column:
    return Column(f"TRIM({_to_col_expr(col_ref)})")


def ltrim(col_ref: str | Column) -> Column:
    return Column(f"LTRIM({_to_col_expr(col_ref)})")


def rtrim(col_ref: str | Column) -> Column:
    return Column(f"RTRIM({_to_col_expr(col_ref)})")


def length(col_ref: str | Column) -> Column:
    return Column(f"LENGTH({_to_col_expr(col_ref)})")


def ascii(col_ref: str | Column) -> Column:
    return Column(f"ASCII({_to_col_expr(col_ref)})")


def instr(col_ref: str | Column, substr: str | Column) -> Column:
    return Column(f"INSTR({_to_col_expr(col_ref)}, {_to_sql_expr(substr)})")


def charindex(substr: str | Column, col_ref: str | Column, position: int | None = None) -> Column:
    """1-based index of ``substr`` in ``col_ref`` (0 when absent, NULL for
    NULL args). ``position`` starts the search (Spark charindex); rendered
    as a native CHARINDEX on the suffix, rebased by ``position - 1``."""
    sub = _to_sql_expr(substr)
    s = _to_col_expr(col_ref)
    if position is None:
        return Column(f"CHARINDEX({sub}, {s})")
    if position < 1:
        raise ValueError(f"position must be >= 1, got {position}")
    return Column(
        f"(CASE WHEN CHARINDEX({sub}, SUBSTRING({s}, {position})) = 0 THEN 0 "
        f"ELSE CHARINDEX({sub}, SUBSTRING({s}, {position})) + {position} - 1 END)"
    )


def startswith(col_ref: str | Column, prefix: str | Column) -> Column:
    """String starts with prefix."""
    return Column(f"%EXACT {_to_col_expr(col_ref)} LIKE {_to_sql_expr(prefix)} || '%'")


def endswith(col_ref: str | Column, suffix: str | Column) -> Column:
    """String ends with suffix."""
    return Column(f"%EXACT {_to_col_expr(col_ref)} LIKE '%' || {_to_sql_expr(suffix)}")


def left(col_ref: str | Column, length_val: int) -> Column:
    return Column(f"LEFT({_to_col_expr(col_ref)}, {_to_sql_expr(length_val)})")


def right(col_ref: str | Column, length_val: int) -> Column:
    return Column(f"RIGHT({_to_col_expr(col_ref)}, {_to_sql_expr(length_val)})")


def repeat(col_ref: str | Column, n: int) -> Column:
    return Column(f"REPEAT({_to_col_expr(col_ref)}, {_to_sql_expr(n)})")


def reverse(col_ref: str | Column) -> Column:
    return Column(f"REVERSE({_to_col_expr(col_ref)})")


def space(n: int) -> Column:
    return Column(f"SPACE({_to_sql_expr(n)})")


def concat(*cols: str | Column) -> Column:
    exprs = [_to_col_expr(c) for c in cols]
    return Column(" || ".join(exprs))


def substring(col_ref: str | Column, pos: int, length_val: int) -> Column:
    return Column(f"SUBSTRING({_to_col_expr(col_ref)}, {_to_sql_expr(pos)}, {_to_sql_expr(length_val)})")


def substr(col_ref: str | Column, pos: int, length_val: int) -> Column:
    return substring(col_ref, pos, length_val)


def replace(src: str | Column, search: str | Column, replacement: str | Column) -> Column:
    return Column(f"REPLACE({_to_col_expr(src)}, {_to_sql_expr(search)}, {_to_sql_expr(replacement)})")


def _pad_sql(func: str, expr: str, length_val: str, pad: str) -> str:
    # IRIS pads with NUL characters when pad is empty; Spark leaves the
    # string unchanged.
    return f"CASE WHEN {pad} = '' THEN {expr} ELSE {func}({expr}, {length_val}, {pad}) END"


def lpad(col_ref: str | Column, length_val: int, pad: str) -> Column:
    expr = _to_col_expr(col_ref)
    return Column(_pad_sql("LPAD", expr, _to_sql_expr(length_val), _to_sql_expr(pad)))


def rpad(col_ref: str | Column, length_val: int, pad: str) -> Column:
    expr = _to_col_expr(col_ref)
    return Column(_pad_sql("RPAD", expr, _to_sql_expr(length_val), _to_sql_expr(pad)))


def greatest(*cols: str | Column) -> Column:
    args = ", ".join(_to_col_expr(c) for c in cols)
    return Column(f"GREATEST({args})")


def least(*cols: str | Column) -> Column:
    args = ", ".join(_to_col_expr(c) for c in cols)
    return Column(f"LEAST({args})")


def coalesce(*cols: str | Column) -> Column:
    args = ", ".join(_to_col_expr(c) for c in cols)
    return CoalesceColumn("COALESCE", list(cols), f"COALESCE({args})")


def isnull(col_ref: str | Column) -> Column:
    expr = _to_col_expr(col_ref)
    return PredicateColumn(
        f"(CASE WHEN {expr} IS NULL THEN 1 ELSE 0 END)",
        f"({expr} IS NULL)",
    )


def isnotnull(col_ref: str | Column) -> Column:
    expr = _to_col_expr(col_ref)
    return PredicateColumn(
        f"(CASE WHEN {expr} IS NOT NULL THEN 1 ELSE 0 END)",
        f"({expr} IS NOT NULL)",
    )


def nullif(col1: str | Column, col2: str | Column) -> Column:
    return Column(f"NULLIF({_to_col_expr(col1)}, {_to_sql_expr(col2)})")


def ifnull(col1: str | Column, col2: str | Column) -> Column:
    # IRIS IFNULL returns NULL for non-null values (SQLCODE -378 quirk),
    # so render as two-argument COALESCE, which behaves correctly.
    return CoalesceColumn(
        "COALESCE", [col1, col2], f"COALESCE({_to_col_expr(col1)}, {_to_sql_expr(col2)})"
    )


def nvl(col1: str | Column, col2: str | Column) -> Column:
    # PySpark aliases nvl() to ifnull(); render as COALESCE for IRIS compatibility.
    return CoalesceColumn(
        "COALESCE", [col1, col2], f"COALESCE({_to_col_expr(col1)}, {_to_sql_expr(col2)})"
    )


def _agg_func(func_name: str) -> Callable[[str | Column], Column]:
    def fn(col_name: str | Column) -> Column:
        # Duck-type on _expr: bare when(...) yields a CaseWhen that is not a
        # Column subclass, and str()-ing it would embed an object repr in SQL.
        if hasattr(col_name, "_expr"):
            col_name = col_name._expr
        return Column(f"{func_name}({col_name})")

    fn.__name__ = func_name
    return fn


avg = _agg_func("AVG")
sum = _agg_func("SUM")
count = _agg_func("COUNT")
min = _agg_func("MIN")
max = _agg_func("MAX")


def mean(col_ref: str | Column) -> Column:
    return Column(f"AVG({_to_col_expr(col_ref)})")


def stddev(col_ref: str | Column) -> Column:
    return Column(f"STDDEV({_to_col_expr(col_ref)})")


std = stddev


def stddev_samp(col_ref: str | Column) -> Column:
    return Column(f"STDDEV_SAMP({_to_col_expr(col_ref)})")


def stddev_pop(col_ref: str | Column) -> Column:
    return Column(f"STDDEV_POP({_to_col_expr(col_ref)})")


def var_samp(col_ref: str | Column) -> Column:
    return Column(f"VAR_SAMP({_to_col_expr(col_ref)})")


variance = var_samp


def var_pop(col_ref: str | Column) -> Column:
    return Column(f"VAR_POP({_to_col_expr(col_ref)})")


def countDistinct(col_ref: str | Column, *cols: str | Column) -> Column:
    if cols:
        args = [_to_col_expr(c) for c in (col_ref,) + cols]
        coalesced = " || chr(0) || ".join(
            f"COALESCE(CAST({a} AS VARCHAR), chr(1))" for a in args
        )
        return Column(f"COUNT(DISTINCT {coalesced})")
    return Column(f"COUNT(DISTINCT {_to_col_expr(col_ref)})")


def sumDistinct(col_ref: str | Column) -> Column:
    return Column(f"SUM(DISTINCT {_to_col_expr(col_ref)})")


def skewness(col_ref: str | Column) -> Column:
    return Column(f"IRISPARK.SKEWNESS({_to_col_expr(col_ref)})")


def kurtosis(col_ref: str | Column) -> Column:
    return Column(f"IRISPARK.KURTOSIS({_to_col_expr(col_ref)})")


def _pairwise_terms(c1: str, c2: str) -> tuple[str, str, str, str, str, str]:
    """Gated aggregate terms for pairwise-complete (null-excluding) statistics.

    SQL aggregates skip NULL inputs, but ``COUNT(*)`` counts all rows, so the
    classic formula mis-counts ``n`` whenever a column contains NULLs (and
    ``SUM(x)``/``SUM(y)`` would use different row sets). Gating every term on
    ``c1 IS NOT NULL AND c2 IS NOT NULL`` gives PySpark/pandas pairwise
    semantics (the governance doc's §5.1/§22.3 requirement).

    Returns (n, sumXY, sumX, sumY, sumX2, sumY2) as SQL fragment strings.
    """
    valid = f"{c1} IS NOT NULL AND {c2} IS NOT NULL"
    return (
        f"SUM(CASE WHEN {valid} THEN 1 ELSE 0 END)",
        f"SUM(CASE WHEN {valid} THEN {c1} * {c2} END)",
        f"SUM(CASE WHEN {valid} THEN {c1} END)",
        f"SUM(CASE WHEN {valid} THEN {c2} END)",
        f"SUM(CASE WHEN {valid} THEN {c1} * {c1} END)",
        f"SUM(CASE WHEN {valid} THEN {c2} * {c2} END)",
    )


def covar_samp(col1: str | Column, col2: str | Column) -> Column:
    c1 = _to_col_expr(col1)
    c2 = _to_col_expr(col2)
    n, sumXY, sumX, sumY, _, _ = _pairwise_terms(c1, c2)
    return Column(f"({n} * {sumXY} - {sumX} * {sumY}) / NULLIF({n} - 1, 0)")


def covar_pop(col1: str | Column, col2: str | Column) -> Column:
    c1 = _to_col_expr(col1)
    c2 = _to_col_expr(col2)
    n, sumXY, sumX, sumY, _, _ = _pairwise_terms(c1, c2)
    return Column(f"({n} * {sumXY} - {sumX} * {sumY}) / {n}")


def corr(col1: str | Column, col2: str | Column) -> Column:
    # Pearson correlation via the computational formula (single division):
    # r = (n*SUM(xy) - SUM(x)*SUM(y)) / sqrt((n*SUM(x^2)-(SUM(x))^2) * (n*SUM(y^2)-(SUM(y))^2))
    # All terms are gated on pairwise non-NULL inputs (native SUM skips NULLs
    # but COUNT(*) would not); IRIS does not accept ^ or ** as exponentiation,
    # use POWER() instead.
    if isinstance(col1, Column):
        col1 = col1._expr
    if isinstance(col2, Column):
        col2 = col2._expr
    n, sumXY, sumX, sumY, sumX2, sumY2 = _pairwise_terms(col1, col2)
    return Column(
        f"({n} * {sumXY} - {sumX} * {sumY}) / "
        f"NULLIF(SQRT(({n} * {sumX2} - POWER({sumX}, 2)) * "
        f"({n} * {sumY2} - POWER({sumY}, 2))), 0)"
    )


def approx_count_distinct(col_ref: str | Column, rsd: float | None = None) -> Column:
    return Column(f"COUNT(DISTINCT {_to_col_expr(col_ref)})")


def year(col_ref: str | Column) -> Column:
    return Column(f"YEAR({_to_col_expr(col_ref)})")


def month(col_ref: str | Column) -> Column:
    return Column(f"MONTH({_to_col_expr(col_ref)})")


def dayofmonth(col_ref: str | Column) -> Column:
    return Column(f"DAY({_to_col_expr(col_ref)})")


day = dayofmonth


def hour(col_ref: str | Column) -> Column:
    return Column(f"HOUR({_to_col_expr(col_ref)})")


def minute(col_ref: str | Column) -> Column:
    return Column(f"MINUTE({_to_col_expr(col_ref)})")


def second(col_ref: str | Column) -> Column:
    return Column(f"SECOND({_to_col_expr(col_ref)})")


def dayofweek(col_ref: str | Column) -> Column:
    return Column(f"DAYOFWEEK({_to_col_expr(col_ref)})")


def dayofyear(col_ref: str | Column) -> Column:
    return Column(f"DAYOFYEAR({_to_col_expr(col_ref)})")


def quarter(col_ref: str | Column) -> Column:
    return Column(f"QUARTER({_to_col_expr(col_ref)})")


def weekofyear(col_ref: str | Column) -> Column:
    return Column(f"WEEK({_to_col_expr(col_ref)})")


def dayname(col_ref: str | Column) -> Column:
    return Column(f"DAYNAME({_to_col_expr(col_ref)})")


def monthname(col_ref: str | Column) -> Column:
    return Column(f"MONTHNAME({_to_col_expr(col_ref)})")


def last_day(col_ref: str | Column) -> Column:
    return Column(f"LAST_DAY({_to_col_expr(col_ref)})")


def current_date() -> Column:
    return Column("CURRENT_DATE")


curdate = current_date


def current_timestamp() -> Column:
    return Column("CURRENT_TIMESTAMP")


now = current_timestamp


def date_add(col_ref: str | Column, days: int) -> Column:
    return Column(f"DATEADD('day', {_to_sql_expr(days)}, {_to_col_expr(col_ref)})")


dateadd = date_add


def date_sub(col_ref: str | Column, days: int) -> Column:
    return Column(f"DATEADD('day', -({_to_sql_expr(days)}), {_to_col_expr(col_ref)})")


def add_months(col_ref: str | Column, months: int) -> Column:
    return Column(f"DATEADD('month', {_to_sql_expr(months)}, {_to_col_expr(col_ref)})")


def datediff(end: str | Column, start: str | Column) -> Column:
    return Column(f"DATEDIFF('day', {_to_col_expr(start)}, {_to_col_expr(end)})")


_TIMESTAMP_DIFF_SECONDS = {
    "WEEK": 604800, "WW": 604800,
    "DAY": 86400, "DD": 86400,
    "HOUR": 3600, "HH": 3600,
    "MINUTE": 60,
    "SECOND": 1,
}

_TIMESTAMP_DIFF_MONTHS = {
    "YEAR": 12, "YYYY": 12, "YY": 12,
    "QUARTER": 3,
    "MONTH": 1, "MM": 1,
}


def timestampdiff(unit: str, start: str | Column, end: str | Column) -> Column:
    """Number of truncated ``unit`` boundaries between ``start`` and ``end``
    (PySpark timestampdiff), truncated toward zero like Spark's integer
    division. YEAR/QUARTER/MONTH are calendar-month based: Spark computes
    ``(months + day_diff/31.0)`` (verified against live PySpark 3.5.8 on
    month-end/leap vectors) and truncates; note timestampdiff does NOT apply
    months_between's "both dates are last-of-month → exact" special case.
    WEEK/DAY/HOUR/MINUTE/SECOND are epoch-second based (DATEDIFF('ss') ÷ n),
    matching Spark's floor semantics for timestamp inputs. NULL → NULL.
    """
    key = unit.strip().upper()
    if key in _TIMESTAMP_DIFF_MONTHS:
        divisor = _TIMESTAMP_DIFF_MONTHS[key]
        s = _to_col_expr(start)
        e = _to_col_expr(end)
        numerator = (
            f"(DATEDIFF('month', {s}, {e})"
            f" + (DAYOFMONTH({e}) - DAYOFMONTH({s})) / 31.0)"
        )
    elif key in _TIMESTAMP_DIFF_SECONDS:
        divisor = _TIMESTAMP_DIFF_SECONDS[key]
        numerator = f"DATEDIFF('ss', {_to_col_expr(start)}, {_to_col_expr(end)})"
    else:
        raise ValueError(f"unsupported unit for timestampdiff: {unit!r}")
    if divisor == 1:
        return Column(f"CAST({numerator} AS INTEGER)")
    return Column(f"CAST(({numerator} / {divisor}) AS INTEGER)")


def months_between(date1: str | Column, date2: str | Column) -> Column:
    """Fractional months between ``date1`` and ``date2`` (PySpark
    months_between date1 - date2 semantics, matched against live PySpark
    3.5.8): calendar months plus the day fraction with a constant 31-day
    denominator, except when BOTH dates are the last day of their month,
    where the result is the exact integer month count (roundOff is always
    FALSE in IRISpark; the both-last rule holds in both Spark modes)."""
    return Column(f"irispark_months_between({_to_col_expr(date2)}, {_to_col_expr(date1)})")


_DOW_MAP: dict[str, int] = {
    "sunday": 1, "sun": 1,
    "monday": 2, "mon": 2,
    "tuesday": 3, "tue": 3,
    "wednesday": 4, "wed": 4,
    "thursday": 5, "thu": 5,
    "friday": 6, "fri": 6,
    "saturday": 7, "sat": 7,
}


def md5(col_ref: str | Column) -> Column:
    return Column(f"md5({_to_col_expr(col_ref)})")


def uuid() -> Column:
    return Column("REPLACE(REPLACE(UUID(), '{', ''), '}', '')")


def pmod(col1: str | Column, col2: str | Column) -> Column:
    a = _to_col_expr(col1)
    b = _to_sql_expr(col2)
    # MOD(MOD(a,b)+b,b) is Spark's pmod for positive b only; for negative b
    # Spark returns the plain remainder (sign of the dividend).
    return Column(f"CASE WHEN {b} > 0 THEN MOD(MOD({a}, {b}) + {b}, {b}) ELSE MOD({a}, {b}) END")


def sha1(col_ref: str | Column) -> Column:
    return Column(f"sha1({_to_col_expr(col_ref)})")


def sha2(col_ref: str | Column, num_bits: int) -> Column:
    return Column(f"sha2({_to_col_expr(col_ref)}, {_to_sql_expr(num_bits)})")


def crc32(col_ref: str | Column) -> Column:
    return Column(f"crc32({_to_col_expr(col_ref)})")


def initcap(col_ref: str | Column) -> Column:
    return Column(f"initcap({_to_col_expr(col_ref)})")


def levenshtein(col1: str | Column, col2: str | Column) -> Column:
    return Column(f"levenshtein({_to_col_expr(col1)}, {_to_col_expr(col2)})")


def soundex(col_ref: str | Column) -> Column:
    return Column(f"soundex({_to_col_expr(col_ref)})")


def regexp_extract(col_ref: str | Column, pattern: str, idx: int = 0) -> Column:
    return Column(f"regexp_extract({_to_col_expr(col_ref)}, {_to_sql_expr(pattern)}, {_to_sql_expr(idx)})")


def collect_list(col_ref: str | Column) -> Column:
    return Column(f"LIST({_to_col_expr(col_ref)})")


def collect_set(col_ref: str | Column) -> Column:
    return Column(f"LIST(DISTINCT {_to_col_expr(col_ref)})")


def unix_timestamp(col_ref: str | Column) -> Column:
    return Column(f"DATEDIFF('second', '1970-01-01 00:00:00', {_to_col_expr(col_ref)})")


def from_unixtime(col_ref: str | Column) -> Column:
    return Column(f"DATEADD('second', {_to_col_expr(col_ref)}, '1970-01-01 00:00:00')")


def percentile_approx(col_ref: str | Column, percentage: float, accuracy: int | None = None) -> Column:
    return Column(
        f"IRISPARK_PERCENTILE_ANALYTIC({_to_col_expr(col_ref)}, {_to_sql_expr(percentage)})"
    )


def median(col_ref: str | Column) -> Column:
    """Median via the SQL-native analytic engine (pandas linear interpolation,
    no state-size limits; expanded by the SQL generator)."""
    return Column(f"IRISPARK_MEDIAN_ANALYTIC({_to_col_expr(col_ref)})")


def percentile(col_ref: str | Column, p: float) -> Column:
    """Quantile at probability ``p`` in [0, 1] via the SQL-native analytic
    engine (pandas linear interpolation, no state-size limits)."""
    if not 0 <= p <= 1:
        raise ValueError(f"percentile probability must be in [0, 1], got {p}")
    return Column(f"IRISPARK_PERCENTILE_ANALYTIC({_to_col_expr(col_ref)}, {p})")


def quantile(col_ref: str | Column, p: float) -> Column:
    """Alias of :func:`percentile` (0-1 scale, matching pandas/SQL semantics)."""
    if not 0 <= p <= 1:
        raise ValueError(f"percentile probability must be in [0, 1], got {p}")
    return Column(f"IRISPARK_QUANTILE_ANALYTIC({_to_col_expr(col_ref)}, {p})")


def next_day(date_col: str | Column, day_of_week: str) -> Column:
    target = _DOW_MAP[day_of_week.lower()]
    expr = _to_sql_expr(date_col)
    return Column(
        f"CASE WHEN DAYOFWEEK({expr}) < {target} "
        f"THEN DATEADD('day', {target} - DAYOFWEEK({expr}), {expr}) "
        f"ELSE DATEADD('day', 7 + {target} - DAYOFWEEK({expr}), {expr}) END"
    )


def regexp_replace(str_col: str | Column, pattern: str, replacement: str) -> Column:
    return Column(
        f"irispark_regexp_replace({_to_col_expr(str_col)}, "
        f"{_to_sql_expr(pattern)}, {_to_sql_expr(replacement)})"
    )


def split(str_col: str | Column, pattern: str) -> Column:
    return Column(f"irispark_split({_to_col_expr(str_col)}, {_to_sql_expr(pattern)})")


def to_date(col_ref: str | Column, fmt: str = "YYYY-MM-DD") -> Column:
    return Column(f"TO_DATE({_to_col_expr(col_ref)}, '{fmt}')")


def date_format(col_ref: str | Column, fmt: str = "YYYY-MM-DD") -> Column:
    return Column(f"TO_CHAR({_to_col_expr(col_ref)}, '{fmt}')")


def rand(seed: int | None = None) -> Column:
    if seed is not None:
        return Column(f"irispark_rand({_to_sql_expr(seed)})")
    import random
    return Column(f"irispark_rand({random.randint(0, 2**31)})")


def randn(seed: int | None = None) -> Column:
    if seed is not None:
        return Column(f"irispark_rand({_to_sql_expr(seed)}) * 2 - 1")
    import random
    return Column(f"irispark_rand({random.randint(0, 2**31)}) * 2 - 1")


def row_number() -> Column:
    return Column("ROW_NUMBER()")


def rank() -> Column:
    return Column("RANK()")


def dense_rank() -> Column:
    return Column("DENSE_RANK()")


def percent_rank() -> Column:
    return Column("PERCENT_RANK()")


def cume_dist() -> Column:
    return Column("CUME_DIST()")


def ntile(n: int) -> Column:
    return Column(f"NTILE({_to_sql_expr(n)})")


def lag(col_ref: str | Column, offset: int = 1, default: Any = None) -> Column:
    if default is None:
        return Column(
            f"LAG({_to_col_expr(col_ref)}, {_to_sql_expr(offset)})"
        )
    return Column(
        f"LAG({_to_col_expr(col_ref)}, {_to_sql_expr(offset)}, {_to_sql_expr(default)})"
    )


def lead(col_ref: str | Column, offset: int = 1, default: Any = None) -> Column:
    if default is None:
        return Column(
            f"LEAD({_to_col_expr(col_ref)}, {_to_sql_expr(offset)})"
        )
    return Column(
        f"LEAD({_to_col_expr(col_ref)}, {_to_sql_expr(offset)}, {_to_sql_expr(default)})"
    )


def first_value(col_ref: str | Column) -> Column:
    return Column(f"FIRST_VALUE({_to_col_expr(col_ref)})")


class _WindowFunction(Column):
    def over(self, window_spec: Any) -> Column:
        from .window import WindowSpec
        if not isinstance(window_spec, WindowSpec):
            raise TypeError(
                f"over() requires a WindowSpec, got {type(window_spec).__name__}"
            )
        ws = window_spec
        if ws._frame is None and ws._order_cols:
            from copy import copy
            ws = copy(window_spec)
            from .window import _unboundedFollowing, _unboundedPreceding
            ws._frame = ("ROWS", _unboundedPreceding, _unboundedFollowing)
        return Column(f"{self._expr} OVER ({ws._to_sql()})")


def last_value(col_ref: str | Column) -> Column:
    return _WindowFunction(f"LAST_VALUE({_to_col_expr(col_ref)})")


def nth_value(col_ref: str | Column, n: int) -> Column:
    return _WindowFunction(f"NTH_VALUE({_to_col_expr(col_ref)}, {_to_sql_expr(n)})")


def explode(col_ref: str | Column) -> Column:
    if isinstance(col_ref, str):
        col_ref = col(col_ref)
    return Column(f"EXPLODE({col_ref._expr})")


def column(name: str) -> Column:
    """Alias of :func:`col`."""
    return Column(name)


def power(col1: str | Column, col2: str | Column) -> Column:
    """Alias of :func:`pow` (PySpark naming)."""
    return pow(col1, col2)


def random(seed: int | None = None) -> Column:
    """Alias of :func:`rand` (PySpark naming)."""
    return rand(seed)


def count_distinct(col_ref: str | Column, *cols: str | Column) -> Column:
    """Snake-case alias of :func:`countDistinct`."""
    return countDistinct(col_ref, *cols)


def sum_distinct(col_ref: str | Column) -> Column:
    """Snake-case alias of :func:`sumDistinct`."""
    return sumDistinct(col_ref)


def var(col_ref: str | Column) -> Column:
    """Alias of :func:`variance` (sample variance)."""
    return variance(col_ref)


def date_diff(end: str | Column, start: str | Column) -> Column:
    """Snake-case alias of :func:`datediff` (days between two dates)."""
    return datediff(end, start)


def e() -> Column:
    """Euler's number as a constant column (PySpark naming)."""
    return Column("EXP(1)")


def pi() -> Column:
    """Pi constant."""
    return Column("3.141592653589793")


def asin(col_ref: str | Column) -> Column:
    """Arc sine; out-of-domain input → NaN, NULL → NULL (Spark semantics;
    native IRIS ASIN fatals with SQLCODE -400 out of domain, so the
    guarded IRISPARK_ASIN helper is used)."""
    return Column(f"irispark_asin({_to_col_expr(col_ref)})")


def acos(col_ref: str | Column) -> Column:
    """Arc cosine; out-of-domain input → NaN, NULL → NULL (Spark semantics;
    native IRIS ACOS fatals with SQLCODE -400 out of domain, so the
    guarded IRISPARK_ACOS helper is used)."""
    return Column(f"irispark_acos({_to_col_expr(col_ref)})")


def acosh(col_ref: str | Column) -> Column:
    x = _to_col_expr(col_ref)
    return Column(f"LOG({x} + SQRT({x} * {x} - 1))")


def atan(col_ref: str | Column) -> Column:
    return Column(f"ATAN({_to_col_expr(col_ref)})")


def atan2(col_y: str | Column, col_x: str | Column) -> Column:
    """Inverse tangent of y/x, using the signs of both (Spark atan2(y, x)).
    NULL args → NULL: native IRIS ATAN2 fatals (SQLCODE -400 "Invalid
    argument") on NULL while Spark propagates NULL, so NULL guards wrap it.
    """
    yy = _to_col_expr(col_y)
    xx = _to_col_expr(col_x)
    return Column(
        f"CASE WHEN {yy} IS NULL OR {xx} IS NULL THEN NULL ELSE ATAN2({yy}, {xx}) END"
    )


def atanh(col_ref: str | Column) -> Column:
    x = _to_col_expr(col_ref)
    return Column(f"(0.5 * (LOG(1 + {x}) - LOG(1 - {x})))")


def sinh(col_ref: str | Column) -> Column:
    x = _to_col_expr(col_ref)
    return Column(f"((EXP({x}) - EXP(-({x}))) / 2)")


def cosh(col_ref: str | Column) -> Column:
    x = _to_col_expr(col_ref)
    return Column(f"((EXP({x}) + EXP(-({x}))) / 2)")


def tanh(col_ref: str | Column) -> Column:
    x = _to_col_expr(col_ref)
    return Column(f"((EXP({x}) - EXP(-({x}))) / (EXP({x}) + EXP(-({x}))))")


def expm1(col_ref: str | Column) -> Column:
    return Column(f"(EXP({_to_col_expr(col_ref)}) - 1)")


def log1p(col_ref: str | Column) -> Column:
    return Column(f"LOG(1 + {_to_col_expr(col_ref)})")


def log2(col_ref: str | Column) -> Column:
    return Column(f"(LOG({_to_col_expr(col_ref)}) / LOG(2))")


def first(col_ref: str | Column, ignorenulls: bool = False) -> Column:
    """First value of the group via the IRISPARK.AGG_FIRST UDAF.

    NOTE: like most SQL engines the UDAF skips NULL inputs; PySpark's default
    ``ignorenulls=False`` (returning NULL when the first row is NULL) is a
    documented deviation.
    """
    return Column(f"IRISPARK.AGG_FIRST({_to_col_expr(col_ref)})")


def last(col_ref: str | Column, ignorenulls: bool = False) -> Column:
    """Last value of the group via the IRISPARK.AGG_LAST UDAF (skips NULLs)."""
    return Column(f"IRISPARK.AGG_LAST({_to_col_expr(col_ref)})")


def max_by(col_ref: str | Column, ord_col: str | Column) -> Column:
    """Value of ``col_ref`` associated with the max of ``ord_col`` (per group)."""
    return Column(f"IRISPARK.AGG_MAX_BY({_to_col_expr(col_ref)}, {_to_col_expr(ord_col)})")


def min_by(col_ref: str | Column, ord_col: str | Column) -> Column:
    """Value of ``col_ref`` associated with the min of ``ord_col`` (per group)."""
    return Column(f"IRISPARK.AGG_MIN_BY({_to_col_expr(col_ref)}, {_to_col_expr(ord_col)})")


def bool_and(col_ref: str | Column) -> Column:
    return Column(f"MIN(CASE WHEN {_predicate_expr(col_ref)} THEN 1 ELSE 0 END)")


def bool_or(col_ref: str | Column) -> Column:
    return Column(f"MAX(CASE WHEN {_predicate_expr(col_ref)} THEN 1 ELSE 0 END)")


any = bool_or
some = bool_or
every = bool_and


def count_if(col_ref: str | Column) -> Column:
    return Column(f"SUM(CASE WHEN {_predicate_expr(col_ref)} THEN 1 ELSE 0 END)")


def approx_percentile(col_ref: str | Column, percentage: float, accuracy: int | None = None) -> Column:
    """Alias of :func:`percentile_approx` (exact SQL-native analytic result)."""
    return percentile_approx(col_ref, percentage, accuracy)


def nanvl(col1: str | Column, col2: str | Column) -> Column:
    c1 = _to_col_expr(col1)
    c2 = _to_col_expr(col2)
    # IRIS has no NaN; this is identity except it folds NULL to NULL (Spark
    # nanvl returns NULL for NULL v1 as well, matched by the first WHEN).
    return Column(f"CASE WHEN {c1} IS NULL THEN {c1} WHEN {c1} = {c1} THEN {c1} ELSE {c2} END")


def nvl2(col_ref: str | Column, v1: Any, v2: Any) -> Column:
    return Column(
        f"CASE WHEN {_to_col_expr(col_ref)} IS NOT NULL THEN {_to_sql_expr(v1)} ELSE {_to_sql_expr(v2)} END"
    )


def zeroifnull(col_ref: str | Column) -> Column:
    return Column(f"COALESCE({_to_col_expr(col_ref)}, 0)")


def nullifzero(col_ref: str | Column) -> Column:
    return Column(f"NULLIF({_to_col_expr(col_ref)}, 0)")


def equal_null(col1: str | Column, col2: str | Column) -> Column:
    c1 = _to_col_expr(col1)
    c2 = _to_col_expr(col2)
    return Column(f"(CASE WHEN ({c1} IS NULL AND {c2} IS NULL) OR ({c1} = {c2}) THEN 1 ELSE 0 END)")


def current_time() -> Column:
    return Column("CURRENT_TIME")


def localtimestamp() -> Column:
    # IRIS timestamps are local; CURRENT_TIMESTAMP is local by definition.
    return Column("CURRENT_TIMESTAMP")


def current_user() -> Column:
    return Column("USER")


def weekday(col_ref: str | Column) -> Column:
    """Day of week, 0=Monday..6=Sunday (PySpark semantics; IRIS DAYOFWEEK is 1=Sunday..)."""
    return Column(f"MOD(DAYOFWEEK({_to_col_expr(col_ref)}) + 5, 7)")


_EXTRACT_FIELDS: dict[str, str] = {
    "YEAR": "YEAR",
    "QUARTER": "QUARTER",
    "MONTH": "MONTH",
    "WEEK": "WEEK",
    "DAY": "DAY",
    "DAYOFWEEK": "DAYOFWEEK",
    "DAYOFYEAR": "DAYOFYEAR",
    "HOUR": "HOUR",
    "MINUTE": "MINUTE",
    "SECOND": "SECOND",
}


def extract(field: str, source: str | Column) -> Column:
    """Extract a datetime part by ``field`` name (PySpark naming)."""
    f = _EXTRACT_FIELDS.get(str(field).upper())
    if f is None:
        raise ValueError(f"unsupported extract field: {field}")
    return Column(f"{f}({_to_col_expr(source)})")


def date_part(field: str, source: str | Column) -> Column:
    """Alias of :func:`extract`."""
    return extract(field, source)


datepart = date_part


def date_from_unix_date(days: str | Column) -> Column:
    return Column(f"DATEADD('day', {_to_col_expr(days)}, '1970-01-01')")


def timestamp_seconds(seconds: str | Column) -> Column:
    return Column(f"DATEADD('second', {_to_col_expr(seconds)}, '1970-01-01 00:00:00')")


def timestamp_millis(millis: str | Column) -> Column:
    return Column(f"DATEADD('millisecond', {_to_col_expr(millis)}, '1970-01-01 00:00:00')")


def timestamp_micros(micros: str | Column) -> Column:
    return Column(f"DATEADD('microsecond', {_to_col_expr(micros)}, '1970-01-01 00:00:00')")


def unix_date(col_ref: str | Column) -> Column:
    return Column(f"DATEDIFF('day', '1970-01-01', {_to_col_expr(col_ref)})")


def unix_seconds(col_ref: str | Column) -> Column:
    return Column(f"DATEDIFF('second', '1970-01-01 00:00:00', {_to_col_expr(col_ref)})")


def unix_millis(col_ref: str | Column) -> Column:
    return Column(f"DATEDIFF('millisecond', '1970-01-01 00:00:00', {_to_col_expr(col_ref)})")


def unix_micros(col_ref: str | Column) -> Column:
    return Column(f"DATEDIFF('microsecond', '1970-01-01 00:00:00', {_to_col_expr(col_ref)})")


def to_unix_timestamp(col_ref: str | Column, fmt: str | None = None) -> Column:
    x = _to_col_expr(col_ref)
    if fmt is None:
        return Column(f"DATEDIFF('second', '1970-01-01 00:00:00', {x})")
    return Column(f"DATEDIFF('second', '1970-01-01', TO_DATE({x}, '{fmt}'))")


_TRUNC_MAP: dict[str, str] = {
    "year": "year", "yyyy": "year", "yy": "year",
    "month": "month", "mon": "month", "mm": "month",
    "quarter": "quarter", "q": "quarter",
    "week": "week", "w": "week",
}


def trunc(col_ref: str | Column, fmt: str) -> Column:
    """Truncate a date to the given unit ('year', 'month', 'quarter', 'week')."""
    unit = _TRUNC_MAP.get(str(fmt).lower())
    if unit is None:
        raise ValueError(f"unsupported trunc unit: {fmt}")
    return Column(f"DATE_TRUNC('{unit}', {_to_col_expr(col_ref)})")


def position(substr: str | Column, str_col: str | Column) -> Column:
    return Column(f"POSITION({_to_sql_expr(substr)} IN {_to_col_expr(str_col)})")


def locate(substr: str | Column, str_col: str | Column, pos: int = 1) -> Column:
    return Column(
        f"INSTR({_to_col_expr(str_col)}, {_to_sql_expr(substr)}, {_to_sql_expr(pos)})"
    )


def concat_ws(sep: str | Column, *cols: str | Column) -> Column:
    """Join columns with a separator. NULL inputs render as empty tokens
    (PySpark skips them; documented deviation requiring a UDF for exactness)."""
    parts = [f"COALESCE(CAST({_to_col_expr(c)} AS VARCHAR), '')" for c in cols]
    return Column(f" || {_to_sql_expr(sep)} || ".join(parts))


def char_length(col_ref: str | Column) -> Column:
    return Column(f"CHAR_LENGTH({_to_col_expr(col_ref)})")


def bit_length(col_ref: str | Column) -> Column:
    return Column(f"CHAR_LENGTH(CAST({_to_col_expr(col_ref)} AS VARCHAR)) * 8")


def chr(col_ref: str | Column) -> Column:
    return Column(f"CHAR({_to_col_expr(col_ref)})")


def elt(n: str | Column, *strs: str | Column) -> Column:
    cases = " ".join(f"WHEN {_to_sql_expr(n)} = {i + 1} THEN {_to_sql_expr(s)}" for i, s in enumerate(strs))
    return Column(f"CASE {cases} END")


def find_in_set(str_col: str | Column, set_col: str | Column) -> Column:
    s = _to_sql_expr(str_col)
    arr = _to_sql_expr(set_col)
    padded = f"',' || {arr} || ','"
    pos = f"INSTR({padded}, ',' || {s} || ',')"
    prefix = f"SUBSTRING({padded}, 1, {pos} - 1)"
    return Column(
        f"CASE WHEN {pos} = 0 THEN 0 ELSE "
        f"CHAR_LENGTH({prefix}) - CHAR_LENGTH(REPLACE({prefix}, ',', '')) + 1 END"
    )


def width_bucket(v: str | Column, min_val: float, max_val: float, num_buckets: int) -> Column:
    x = _to_sql_expr(v)
    lo, hi, nb = str(min_val), str(max_val), str(num_buckets)
    return Column(
        f"CASE WHEN {x} < {lo} OR {hi} <= {lo} THEN 0 "
        f"WHEN {x} >= {hi} THEN {nb} + 1 "
        f"ELSE FLOOR(({x} - {lo}) * {nb} / ({hi} - {lo})) + 1 END"
    )


def uniform(seed: int | None, low: float, high: float) -> Column:
    """Uniform random value in [low, high) — PySpark naming."""
    if seed is None:
        import random as _random
        seed = _random.randint(0, 2**31)
    return Column(f"({low} + irispark_rand({_to_sql_expr(seed)}) * ({high} - {low}))")


def stack(n: int, *pairs: Any) -> Column:
    if len(pairs) != n * 2:
        raise ValueError(
            f"stack({n}, ...) requires exactly {n * 2} arguments "
            f"(n label/value pairs), got {len(pairs)}"
        )
    parts: list[str] = []
    stack_pairs: list[tuple[str, str]] = []
    for i in range(0, len(pairs), 2):
        label = _quote(pairs[i])
        col_expr = _to_sql_expr(pairs[i + 1])
        parts.append(f"({label}, {col_expr})")
        stack_pairs.append((str(pairs[i]), col_expr))
    col = Column(f"STACK({n}, {', '.join(parts)})")
    col._stack_pairs = stack_pairs  # type: ignore[attr-defined]
    return col


def udf(fn: Callable | None = None, *, returnType: DataType | None = None) -> Callable:
    """
    Register a Python function as an IRIS SQL UDF.

    Usage:
        # As decorator with explicit return type
        @udf(returnType=StringType())
        def my_udf(x: int) -> str:
            return str(x * 2)

        # With explicit session
        spark.udf.register("my_udf", lambda x: x * 2, IntegerType())
    """
    if fn is None:
        def decorator(f: Callable) -> Callable:
            return udf(f, returnType=returnType)
        return decorator

    # Get active session
    session = get_active_session()
    if session is None:
        raise RuntimeError(
            "No active IrisParkSession. Create a session first or use "
            "session.udf.register() directly."
        )

    session.udf.register(fn.__name__, fn, returnType)
    return fn


# ---------------------------------------------------------------------------
# irispark_udc wrappers (Embedded Python / ObjectScript UDFs)
# ---------------------------------------------------------------------------

def conv(num: str | Column, frombase: int | Column, tobase: int | Column) -> Column:
    """Convert number between bases (2–36)."""
    return Column(
        f"irispark_udc_conv({_to_sql_expr(num)}, {_to_sql_expr(frombase)}, "
        f"{_to_sql_expr(tobase)})"
    )


def format_string(
    fmt: str | Column,
    *args: str | Column,
) -> Column:
    """Python-style format string."""
    _args = list(args) + [None] * (7 - len(args))
    exprs = ["NULL" if a is None else _to_sql_expr(a) for a in _args[:7]]
    return Column(
        f"irispark_udc_format_string({_to_sql_expr(fmt)}, {', '.join(exprs)})"
    )


def printf(
    fmt: str | Column,
    *args: str | Column,
) -> Column:
    """C-style printf formatting."""
    _args = list(args) + [None] * (7 - len(args))
    exprs = ["NULL" if a is None else _to_sql_expr(a) for a in _args[:7]]
    return Column(
        f"irispark_udc_printf({_to_sql_expr(fmt)}, {', '.join(exprs)})"
    )


def parse_url(url: str | Column, part: str | Column) -> Column:
    """Extract a part from a URL."""
    return Column(
        f"irispark_udc_parse_url({_to_sql_expr(url)}, {_to_sql_expr(part)})"
    )


def parse_url_key(
    url: str | Column, part: str | Column, key: str | Column
) -> Column:
    """Extract a query parameter from a URL."""
    return Column(
        f"irispark_udc_parse_url_key({_to_sql_expr(url)}, "
        f"{_to_sql_expr(part)}, {_to_sql_expr(key)})"
    )


def from_utc_timestamp(ts: str | Column, tz: str | Column) -> Column:
    """Convert UTC timestamp to a timezone."""
    return Column(
        f"irispark_udc_from_utc_timestamp({_to_sql_expr(ts)}, {_to_sql_expr(tz)})"
    )


def to_utc_timestamp(ts: str | Column, tz: str | Column) -> Column:
    """Convert a timezone timestamp to UTC."""
    return Column(
        f"irispark_udc_to_utc_timestamp({_to_sql_expr(ts)}, {_to_sql_expr(tz)})"
    )
