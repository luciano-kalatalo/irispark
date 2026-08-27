from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FunctionDefinition:
    name: str
    pyspark_name: str
    category: str
    execution: str  # native_sql | sql_composition | objectscript | embedded_python | python_fallback
    compatibility: str  # A | B | C | D | E
    status: str  # Supported | Partial | R&D | Unsupported
    columnar_friendly: bool
    vector_candidate: bool
    objectscript_fallback: bool
    python_fallback: bool
    notes: str = ""


# ──────────────────────────────────────────────────────────────────────────────
# Registry of all IrisPark SQL functions / UDFs / UDAFs
# ──────────────────────────────────────────────────────────────────────────────
FUNCTIONS: dict[str, FunctionDefinition] = {}


def _reg(
    name: str,
    *,
    pyspark_name: str | None = None,
    category: str = "misc",
    execution: str = "native_sql",
    compatibility: str = "A",
    status: str = "Supported",
    columnar_friendly: bool = True,
    vector_candidate: bool = True,
    objectscript_fallback: bool = False,
    python_fallback: bool = False,
    notes: str = "",
) -> None:
    FUNCTIONS[name] = FunctionDefinition(
        name=name,
        pyspark_name=pyspark_name or name,
        category=category,
        execution=execution,
        compatibility=compatibility,
        status=status,
        columnar_friendly=columnar_friendly,
        vector_candidate=vector_candidate,
        objectscript_fallback=objectscript_fallback,
        python_fallback=python_fallback,
        notes=notes,
    )


# ── Math ──────────────────────────────────────────────────────────────────────
_reg("abs", category="math")
_reg("acos", category="math", execution="sql_composition",
     notes="Guarded against domain errors (IRIS ACOS fatals out-of-domain)")
_reg("acosh", category="math", execution="sql_composition",
     notes="Log formulation")
_reg("asin", category="math", execution="sql_composition",
     notes="Guarded against domain errors (IRIS ASIN fatals out-of-domain)")
_reg("atan", category="math")
_reg("atan2", category="math", execution="sql_composition", compatibility="A",
     notes="NULL guards (IRIS ATAN2 fatals on NULL)")
_reg("atanh", category="math", execution="sql_composition")
_reg("ceil", category="math", pyspark_name="ceil",
     notes="IRIS CEILING")
_reg("ceiling", category="math", pyspark_name="ceil",
     notes="Alias of ceil")
_reg("cos", category="math")
_reg("cosh", category="math", execution="sql_composition")
_reg("degrees", category="math")
_reg("e", category="math", execution="sql_composition",
     notes="Constant column EXP(1)")
_reg("exp", category="math")
_reg("expm1", category="math", execution="sql_composition",
     notes="EXP(x) - 1")
_reg("floor", category="math")
_reg("ln", category="math")
_reg("log", category="math", execution="sql_composition",
     notes="Two-arg form uses change-of-base formula")
_reg("log10", category="math")
_reg("log1p", category="math", execution="sql_composition")
_reg("log2", category="math", execution="sql_composition")
_reg("negative", category="math")
_reg("negate", category="math", pyspark_name="negative",
     notes="Alias of negative")
_reg("pi", category="math", execution="sql_composition",
     notes="Constant 3.141592653589793")
_reg("pmod", category="math", execution="sql_composition",
     notes="Matches Spark pmod exactly (verified)")
_reg("positive", category="math")
_reg("pow", category="math", execution="sql_composition", compatibility="B",
     notes="Guards POWER(0,negative) and POWER(negative,fractional) to NULL (IRIS cannot represent Inf/NaN)")
_reg("power", category="math", pyspark_name="pow",
     notes="Alias of pow")
_reg("radians", category="math")
_reg("rand", category="math", execution="sql_composition", compatibility="B",
     notes="Uses irispark_rand ObjectScript helper; non-deterministic without seed")
_reg("randn", category="math", execution="sql_composition", compatibility="B",
     notes="Linear transform of rand; not true normal distribution")
_reg("random", category="math", pyspark_name="rand",
     notes="Alias of rand")
_reg("round", category="math")
_reg("sign", category="math")
_reg("signum", category="math", pyspark_name="sign",
     notes="Alias of sign")
_reg("sin", category="math")
_reg("sinh", category="math", execution="sql_composition")
_reg("sqrt", category="math")
_reg("tan", category="math")
_reg("tanh", category="math", execution="sql_composition")
_reg("uniform", category="math", execution="sql_composition", compatibility="B",
     notes="Uniform via irispark_rand; not cryptographically secure")

# ── String ────────────────────────────────────────────────────────────────────
_reg("ascii", category="string")
_reg("bit_length", category="string", execution="sql_composition")
_reg("char_length", category="string")
_reg("chr", category="string")
_reg("concat", category="string", execution="sql_composition",
     notes="Uses || operator")
_reg("concat_ws", category="string", execution="sql_composition", compatibility="B",
     notes="NULL inputs render as empty string (PySpark skips them)")
_reg("elt", category="string", execution="sql_composition")
_reg("find_in_set", category="string", execution="sql_composition")
_reg("initcap", category="string")
_reg("instr", category="string")
_reg("lcase", category="string", pyspark_name="lower",
     notes="Alias of lower")
_reg("left", category="string")
_reg("length", category="string")
_reg("levenshtein", category="string")
_reg("locate", category="string")
_reg("lower", category="string")
_reg("lpad", category="string", execution="sql_composition",
     notes="Guards empty pad string (IRIS pads with NUL)")
_reg("ltrim", category="string")
_reg("position", category="string")
_reg("regexp_extract", category="string")
_reg("regexp_replace", category="string", execution="embedded_python",
     notes="irispark_regexp_replace ObjectScript helper")
_reg("repeat", category="string")
_reg("replace", category="string")
_reg("reverse", category="string")
_reg("right", category="string")
_reg("rpad", category="string", execution="sql_composition",
     notes="Guards empty pad string (IRIS pads with NUL)")
_reg("rtrim", category="string")
_reg("soundex", category="string")
_reg("space", category="string")
_reg("split", category="string", execution="embedded_python",
     notes="irispark_split ObjectScript helper")
_reg("startswith", category="string", execution="sql_composition",
     notes="%EXACT ... LIKE prefix||'%'")
_reg("endswith", category="string", execution="sql_composition",
     notes="%EXACT ... LIKE '%'||suffix")
_reg("substr", category="string", pyspark_name="substring",
     notes="Alias of substring")
_reg("substring", category="string")
_reg("trim", category="string")
_reg("ucase", category="string", pyspark_name="upper",
     notes="Alias of upper")
_reg("upper", category="string")
_reg("width_bucket", category="string", execution="sql_composition")

# ── Date/Time ─────────────────────────────────────────────────────────────────
_reg("add_months", category="datetime", execution="sql_composition")
_reg("current_date", category="datetime")
_reg("current_time", category="datetime")
_reg("current_timestamp", category="datetime")
_reg("curdate", category="datetime", pyspark_name="current_date",
     notes="Alias of current_date")
_reg("date_add", category="datetime")
_reg("date_diff", category="datetime", pyspark_name="datediff",
     notes="Alias of datediff")
_reg("date_format", category="datetime")
_reg("date_from_unix_date", category="datetime", execution="sql_composition")
_reg("date_part", category="datetime", pyspark_name="date_part",
     notes="Alias of extract")
_reg("date_sub", category="datetime")
_reg("datediff", category="datetime")
_reg("day", category="datetime", pyspark_name="dayofmonth",
     notes="Alias of dayofmonth")
_reg("dayname", category="datetime")
_reg("dayofmonth", category="datetime")
_reg("dayofweek", category="datetime")
_reg("dayofyear", category="datetime")
_reg("extract", category="datetime")
_reg("from_unixtime", category="datetime", execution="sql_composition")
_reg("hour", category="datetime")
_reg("last_day", category="datetime")
_reg("localtimestamp", category="datetime", execution="sql_composition",
     notes="Alias of CURRENT_TIMESTAMP (IRIS timestamps are local)")
_reg("minute", category="datetime")
_reg("month", category="datetime")
_reg("monthname", category="datetime")
_reg("months_between", category="datetime", execution="sql_composition", compatibility="A",
     notes="Matched against live PySpark 3.5.8; both-last-day exact rule applied")
_reg("next_day", category="datetime", execution="sql_composition")
_reg("now", category="datetime", pyspark_name="current_timestamp",
     notes="Alias of current_timestamp")
_reg("quarter", category="datetime")
_reg("second", category="datetime")
_reg("timestamp_millis", category="datetime", execution="sql_composition")
_reg("timestamp_micros", category="datetime", execution="sql_composition")
_reg("timestamp_seconds", category="datetime", execution="sql_composition")
_reg("timestampdiff", category="datetime", execution="sql_composition", compatibility="A",
     notes="Matched against live PySpark 3.5.8 on month-end/leap vectors")
_reg("to_date", category="datetime")
_reg("to_unix_timestamp", category="datetime", execution="sql_composition")
_reg("trunc", category="datetime", execution="sql_composition",
     notes="year/month/quarter/week only")
_reg("unix_date", category="datetime", execution="sql_composition")
_reg("unix_micros", category="datetime", execution="sql_composition")
_reg("unix_millis", category="datetime", execution="sql_composition")
_reg("unix_seconds", category="datetime", execution="sql_composition")
_reg("unix_timestamp", category="datetime", execution="sql_composition")
_reg("weekday", category="datetime", execution="sql_composition",
     notes="0=Monday..6=Sunday (IRIS DAYOFWEEK is 1=Sunday..)")
_reg("weekofyear", category="datetime")
_reg("year", category="datetime")

# ── Aggregate ─────────────────────────────────────────────────────────────────
_reg("approx_count_distinct", category="aggregate", execution="sql_composition",
     notes="Exact COUNT(DISTINCT) — no HyperLogLog approximation yet")
_reg("approx_percentile", category="aggregate", execution="sql_composition",
     notes="Alias of percentile_approx")
_reg("avg", category="aggregate")
_reg("bool_and", category="aggregate", execution="sql_composition",
     notes="MIN(CASE WHEN ... THEN 1 ELSE 0)")
_reg("bool_or", category="aggregate", execution="sql_composition",
     notes="MAX(CASE WHEN ... THEN 1 ELSE 0)")
_reg("count", category="aggregate")
_reg("countDistinct", category="aggregate", execution="sql_composition",
     notes="Multi-col form concatenates with chr(0)")
_reg("count_distinct", category="aggregate", pyspark_name="countDistinct",
     notes="Snake-case alias")
_reg("count_if", category="aggregate", execution="sql_composition",
     notes="SUM(CASE WHEN ... THEN 1 ELSE 0)")
_reg("covar_pop", category="aggregate", execution="sql_composition", compatibility="A",
     notes="Pairwise-complete (null-excluding); matched against PySpark")
_reg("covar_samp", category="aggregate", execution="sql_composition", compatibility="A",
     notes="Pairwise-complete (null-excluding); matched against PySpark")
_reg("first", category="aggregate", execution="objectscript", compatibility="B",
     notes="IRISPARK.AGG_FIRST UDAF; documented ignorenulls deviation")
_reg("kurtosis", category="aggregate", execution="objectscript", compatibility="B",
     notes="IRISPARK.KURTOSIS UDAF (ObjectScript)")
_reg("last", category="aggregate", execution="objectscript", compatibility="B",
     notes="IRISPARK.AGG_LAST UDAF; documented ignorenulls deviation")
_reg("max", category="aggregate")
_reg("max_by", category="aggregate", execution="objectscript", compatibility="A",
     notes="IRISPARK.AGG_MAX_BY UDAF")
_reg("mean", category="aggregate",
     notes="Alias of AVG")
_reg("median", category="aggregate", execution="objectscript",
     notes="IRISPARK_MEDIAN_ANALYTIC (analytic engine)")
_reg("min", category="aggregate")
_reg("min_by", category="aggregate", execution="objectscript", compatibility="A",
     notes="IRISPARK.AGG_MIN_BY UDAF")
_reg("percentile", category="aggregate", execution="objectscript",
     notes="IRISPARK_PERCENTILE_ANALYTIC (analytic engine)")
_reg("percentile_approx", category="aggregate", execution="objectscript",
     notes="IRISPARK_PERCENTILE_ANALYTIC (analytic engine)")
_reg("quantile", category="aggregate", execution="objectscript",
     notes="IRISPARK_QUANTILE_ANALYTIC (analytic engine)")
_reg("skewness", category="aggregate", execution="objectscript", compatibility="B",
     notes="IRISPARK.SKEWNESS UDAF (ObjectScript)")
_reg("std", category="aggregate", pyspark_name="stddev",
     notes="Alias of stddev")
_reg("stddev", category="aggregate")
_reg("stddev_pop", category="aggregate")
_reg("stddev_samp", category="aggregate")
_reg("sum", category="aggregate")
_reg("sumDistinct", category="aggregate",
     notes="SUM(DISTINCT)")
_reg("sum_distinct", category="aggregate", pyspark_name="sumDistinct",
     notes="Snake-case alias")
_reg("var_pop", category="aggregate")
_reg("var_samp", category="aggregate")
_reg("variance", category="aggregate", pyspark_name="var_samp",
     notes="Alias of var_samp")
_reg("var", category="aggregate", pyspark_name="var_samp",
     notes="Alias of var_samp")

# ── Window / Analytic ─────────────────────────────────────────────────────────
_reg("cume_dist", category="window")
_reg("dense_rank", category="window")
_reg("first_value", category="window")
_reg("lag", category="window")
_reg("last_value", category="window")
_reg("lead", category="window")
_reg("ntile", category="window")
_reg("percent_rank", category="window")
_reg("rank", category="window")
_reg("row_number", category="window")
_reg("nth_value", category="window", execution="sql_composition")

_reg("charindex", category="string", execution="sql_composition",
     notes="CHARINDEX with position-rebased search")
_reg("greatest", category="string")
_reg("least", category="string")
_reg("collect_list", category="string")
_reg("collect_set", category="string")
_reg("any", category="conditional", pyspark_name="bool_or",
     notes="Alias of bool_or")
_reg("every", category="conditional", pyspark_name="bool_and",
     notes="Alias of bool_and")
_reg("some", category="conditional", pyspark_name="bool_or",
     notes="Alias of bool_or")
_reg("dateadd", category="datetime", pyspark_name="date_add",
     notes="Alias of date_add")
_reg("datepart", category="datetime", pyspark_name="date_part",
     notes="Alias of date_part / extract")
_reg("conv", category="udc", execution="embedded_python", compatibility="A",
     notes="Pure-Python base conversion; overflow semantics match Spark")
_reg("format_string", category="udc", execution="embedded_python", compatibility="A",
     notes="Python-style formatting via irispark_udc.py")
_reg("printf", category="udc", execution="embedded_python", compatibility="A",
     notes="C-style printf via irispark_udc.py")
_reg("parse_url", category="udc", execution="embedded_python", compatibility="A",
     notes="urllib.parse via irispark_udc.py")
_reg("parse_url_key", category="udc", execution="embedded_python", compatibility="A",
     notes="Query-param extraction via irispark_udc.py")
_reg("from_utc_timestamp", category="udc", execution="embedded_python", compatibility="A",
     notes="zoneinfo via irispark_udc.py")
_reg("to_utc_timestamp", category="udc", execution="embedded_python", compatibility="A",
     notes="zoneinfo via irispark_udc.py")

# ── UDAF (ObjectScript) ───────────────────────────────────────────────────────
_reg("corr", category="udaf", execution="objectscript", compatibility="B",
     notes="IRISPARK.CORR UDAF (Welford online algorithm); SQL composition also available")

# ── Conditional / Null ────────────────────────────────────────────────────────
_reg("coalesce", category="conditional")
_reg("equal_null", category="conditional", execution="sql_composition",
     notes="NULL-safe equality")
_reg("ifnull", category="conditional", execution="sql_composition",
     notes="Renders as COALESCE (IRIS IFNULL has quirk)")
_reg("isnotnull", category="conditional", execution="sql_composition",
     notes="PredicateColumn dual form")
_reg("isnull", category="conditional", execution="sql_composition",
     notes="PredicateColumn dual form")
_reg("nanvl", category="conditional", execution="sql_composition",
     notes="IRIS has no NaN; identity for non-NULL")
_reg("nullif", category="conditional")
_reg("nullifzero", category="conditional")
_reg("nvl", category="conditional", execution="sql_composition",
     notes="Alias of ifnull; renders as COALESCE")
_reg("nvl2", category="conditional", execution="sql_composition")
_reg("when", category="conditional", execution="sql_composition",
     notes="CASE WHEN chain")
_reg("zeroifnull", category="conditional", execution="sql_composition")

# ── Misc ──────────────────────────────────────────────────────────────────────
_reg("broadcast", category="misc", execution="sql_composition",
     notes="No-op (IRIS has no broadcast hint)")
_reg("cast", category="misc")
_reg("col", category="misc",
     notes="Column reference constructor")
_reg("column", category="misc", pyspark_name="col",
     notes="Alias of col")
_reg("current_user", category="misc")
_reg("desc", category="misc",
     notes="Descending sort specifier")
_reg("asc", category="misc",
     notes="Ascending sort specifier")
_reg("explode", category="misc",
     notes="EXPLODE() lateral operator")
_reg("expr", category="misc",
     notes="Raw SQL expression")
_reg("lit", category="misc",
     notes="Literal value column")
_reg("md5", category="misc")
_reg("sha1", category="misc")
_reg("sha2", category="misc")
_reg("crc32", category="misc")
_reg("uuid", category="misc", execution="sql_composition",
     notes="IRIS UUID with brace stripping")
_reg("udf", category="misc", execution="python_fallback", compatibility="A",
     notes="@udf decorator registers Python functions as IRIS SQL UDFs")
_reg("stack", category="misc", execution="sql_composition",
     notes="STACK() lateral operator")


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def list_functions(category: str | None = None) -> list[FunctionDefinition]:
    """Return registered functions, optionally filtered by category."""
    if category is None:
        return list(FUNCTIONS.values())
    return [f for f in FUNCTIONS.values() if f.category == category]


def get_function(name: str) -> FunctionDefinition | None:
    """Look up a single function by its IrisPark name."""
    return FUNCTIONS.get(name)


def explain_function(name: str) -> str:
    """Human-readable description of a function's capability."""
    fn = get_function(name)
    if fn is None:
        return f"Function '{name}' is not registered."
    lines = [
        f"{fn.name}  (PySpark: {fn.pyspark_name})",
        f"  Category:      {fn.category}",
        f"  Status:        {fn.status}",
        f"  Compatibility: {fn.compatibility}",
        f"  Execution:     {fn.execution}",
        f"  Columnar:      {fn.columnar_friendly}",
        f"  Vector cand.:  {fn.vector_candidate}",
        f"  OS fallback:   {fn.objectscript_fallback}",
        f"  Py fallback:   {fn.python_fallback}",
    ]
    if fn.notes:
        lines.append(f"  Notes:         {fn.notes}")
    return "\n".join(lines)


def _compat_legend() -> str:
    return (
        "| Level | Meaning |\n"
        "| ----- | ------- |\n"
        "| **A** | Exact target compatibility (tested against PySpark) |\n"
        "| **B** | Operational compatibility (common cases equivalent, documented edge-case deviations) |\n"
        "| **C** | Syntax compatibility (API shape preserved, semantics differ) |\n"
        "| **D** | Partial support (specific modes or parameters only) |\n"
        "| **E** | Unsupported (intentionally not implemented) |\n"
    )


def generate_compatibility_markdown() -> str:
    """Render the full public compatibility matrix (§50 format)."""
    header = (
        "# IrisPark Compatibility Matrix\n\n"
        "This document lists every IrisPark SQL function, its PySpark mapping, "
        "execution engine, and compatibility level.\n\n"
        "## Compatibility Levels\n\n"
        + _compat_legend()
        + "\n## Matrix\n\n"
        "| IrisPark Name | PySpark Name | Category | Status | Compatibility | Execution | Notes |\n"
        "| ------------- | ------------ | -------- | ------ | ------------- | --------- | ----- |\n"
    )
    rows: list[str] = []
    for fn in sorted(FUNCTIONS.values(), key=lambda f: (f.category, f.name)):
        rows.append(
            f"| `{fn.name}` | `{fn.pyspark_name}` | {fn.category} | {fn.status} | "
            f"{fn.compatibility} | {fn.execution} | {fn.notes or '—'} |"
        )
    return header + "\n".join(rows) + "\n"


def generate_migration_markdown() -> str:
    """Render the PySpark → IrisPark migration guide (§79)."""
    works_unchanged: list[str] = []
    import_only: list[str] = []
    minor_adaptation: list[str] = []
    iris_specific: list[str] = []
    unsupported: list[str] = []

    for fn in FUNCTIONS.values():
        if fn.compatibility == "A" and fn.execution in ("native_sql", "sql_composition"):
            works_unchanged.append(fn.name)
        elif fn.compatibility == "A" and fn.execution in ("embedded_python", "objectscript"):
            import_only.append(fn.name)
        elif fn.compatibility == "B":
            minor_adaptation.append(fn.name)
        elif fn.execution == "python_fallback":
            iris_specific.append(fn.name)
        elif fn.compatibility in ("D", "E"):
            unsupported.append(fn.name)

    def _ul(items: list[str]) -> str:
        if not items:
            return "_None currently._\n"
        return "\n".join(f"- `{name}`" for name in sorted(items)) + "\n"

    return (
        "# PySpark → IrisPark Migration Guide\n\n"
        "This guide classifies PySpark code by the amount of change needed to run on IrisPark.\n\n"
        "## 1. Works Unchanged\n\n"
        "Standard SQL functions with exact semantics. Just change the import:\n\n"
        "```python\n"
        "from irispark.functions import sum, avg, upper, lower, year, month, dayofmonth\n"
        "```\n\n"
        + _ul(works_unchanged)
        + "\n## 2. Import Change Only\n\n"
        "Functions that work identically but live in a non-standard execution path "
        "(Embedded Python UDC or ObjectScript UDAF). No code changes beyond import:\n\n"
        + _ul(import_only)
        + "\n## 3. Minor Adaptation Required\n\n"
        "Functions that are operationally compatible but have documented edge-case deviations:\n\n"
        + _ul(minor_adaptation)
        + "\n## 4. IRIS-Specific Adaptation\n\n"
        "Features that require understanding IRIS-specific behaviour or are IrisPark extensions:\n\n"
        + _ul(iris_specific)
        + "\n## 5. Unsupported\n\n"
        "Functions or modes that are intentionally not implemented:\n\n"
        + _ul(unsupported)
        + "\n---\n"
        "_Last updated: auto-generated from `irispark/registry.py`._\n"
    )
