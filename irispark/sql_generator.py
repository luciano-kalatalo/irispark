from __future__ import annotations

import re
from itertools import combinations
from typing import TYPE_CHECKING, Any

from .column import Column, SortColumn, _quote

if TYPE_CHECKING:
    from .dataframe import IrisDataFrame


_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")
_SAFE_LIMIT_RE = re.compile(r"^\d+$")
_SAFE_FILTER_RE = re.compile(r"^[\w.,='()<>!+\-*/%\s\[\]\|^]+$", re.UNICODE)

_UDF_AGG_MAP: dict[str, str] = {
    "kurtosis": "IRISPARK.KURTOSIS({})",
    "skewness": "IRISPARK.SKEWNESS({})",
}

_AGG_FUNCS: set[str] = {
    "sum", "count", "avg", "min", "max", "mean",
    "stddev", "variance", "stddev_samp", "var_samp",
    *_UDF_AGG_MAP.keys(),
}

# Identifiers that IRIS treats as reserved words; unquoted use fails with
# "IDENTIFIER expected, reserved word X found". These must be double-quoted
# when used as column aliases or ORDER BY references. Synced against the
# official InterSystems IRIS 2026.2 reserved-word list (RSQL_reservedwords),
# excluding "%"-prefixed proprietary tokens (never valid identifiers).
_IRIS_RESERVED_WORDS: set[str] = {
    "COUNT", "SUM", "AVG", "MIN", "MAX", "ALL", "ANY", "AS", "BETWEEN",
    "BY", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "CONSTRAINT",
    "CREATE", "CURRENT", "DATABASE", "DEFAULT", "DELETE", "DISTINCT",
    "ELSE", "EXISTS", "FALSE", "FOR", "FROM", "FULL", "GROUP", "HAVING",
    "IN", "INDEX", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN",
    "KEY", "LEFT", "LIKE", "NOT", "NULL", "ON", "OR", "ORDER", "OUTER",
    "PRIMARY", "REFERENCES", "RIGHT", "SELECT", "SET", "SYSTEM", "TABLE",
    "THEN", "TO", "TRUE", "UNION", "UNIQUE", "UPDATE", "USER", "USING",
    "UPPER", "LOWER", "LCASE", "UCASE", "TRIM", "LTRIM", "RTRIM", "LENGTH",
    "CONCAT", "CONCAT_WS", "SUBSTRING", "SUBSTR", "LPAD", "RPAD", "REPEAT",
    "REVERSE", "REPLACE", "INITCAP", "LEFT", "RIGHT", "ASCII", "CHAR",
    "CHAR_LENGTH", "CHARLENGTH", "LEVENSHTEIN", "SOUNDEX", "SPLIT",
    "CHARINDEX", "POSITION", "INSTR", "FIND_IN_SET", "LOCATE", "ELT",
    "EXTRACT",
    "TO_DATE", "TO_CHAR", "DATEADD", "DATEDIFF", "MONTHS_BETWEEN",
    "TIMESTAMPDIFF", "LAST_DAY", "DAYNAME", "MONTHNAME", "DAYOFWEEK",
    "DAYOFYEAR", "DAYOFMONTH", "WEEK", "QUARTER", "GREATEST", "LEAST",
    "COALESCE", "NULLIF", "NVL", "IFNULL", "POWER", "SQRT", "FLOOR",
    "CEILING", "ROUND", "ABS", "EXP", "LOG", "SIN", "COS", "TAN",
    "DOMAIN", "KEYWORD", "SCHEMA", "CLUSTER", "COMMENT", "CONNECT",
    "CURSOR", "DECLARE", "EXECUTE", "EXTERNAL", "FETCH", "GRANT",
    "INTO", "OFFSET", "OPEN", "OPTION", "PATH", "PRIVILEGES", "PROCEDURE",
    "PUBLIC", "REVOKE", "ROLE", "ROWS", "SAVEPOINT", "SESSION", "START",
    "TRIGGER", "VALIDATE",
    # Official IRIS 2026.2 reserved words missing from earlier revisions.
    # FIRST/LAST discovered via notebook crashes (LESSONS_LEARNED UDAF entry).
    "FIRST", "LAST", "TOP", "WHEN", "WHERE", "AND", "VALUES", "WITH",
    "EXCEPTION", "EXEC", "FLOAT", "FOREIGN", "FOUND", "GET", "GLOBAL",
    "GO", "GOTO", "HOUR", "IDENTITY", "IMMEDIATE", "INDICATOR",
    "INITIALLY", "INPUT", "INSENSITIVE", "INT", "INTEGER", "INTERVAL",
    "ISOLATION", "LANGUAGE", "LEADING", "LEVEL", "LOCAL", "MATCH",
    "MINUTE", "MODULE", "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NEXT",
    "NO", "NUMERIC", "OCTET_LENGTH", "OF", "ONLY", "OUTPUT", "OVERLAPS",
    "PAD", "PARTIAL", "PREPARE", "PRESERVE", "PRIOR", "READ", "REAL",
    "RELATIVE", "RESTRICT", "ROLLBACK", "SCROLL", "SECOND", "SECTION",
    "SESSION_USER", "SHARD", "SMALLINT", "SOME", "SPACE", "SQLERROR",
    "SQLSTATE", "STATISTICS", "SYSDATE", "SYSTEM_USER", "TEMPORARY",
    "TIME", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TRAILING",
    "TRANSACTION", "VARCHAR", "VARYING", "WHENEVER", "WORK", "WRITE",
}

_IRIS_RESERVED_LOWER = {w.lower() for w in _IRIS_RESERVED_WORDS}


def _quote_if_reserved(name: str) -> str:
    """Double-quote an identifier if IRIS treats it as a reserved word.

    IRIS uppercases unquoted identifiers, so ``count`` becomes ``COUNT`` and
    is parsed as the aggregate function. Quoting preserves the literal name.
    """
    if name.lower() in _IRIS_RESERVED_LOWER:
        return f'"{name}"'
    return name


def _quote_expr_alias(expr: str) -> str:
    """Quote a reserved-word alias in a ``... AS <alias>`` expression.

    ``Column.alias("upper")`` renders ``UPPER(nome) AS upper``; ``upper`` is an
    IRIS SQL built-in function name and must be double-quoted as a column alias
    or IRIS rejects the statement (``IDENTIFIER expected, reserved word UPPER
    found``). Splits on the last `` AS `` and quotes the alias identifier.
    """
    idx = expr.rfind(" AS ")
    if idx == -1:
        return expr
    head, alias = expr[:idx], expr[idx + 4 :]
    return f"{head} AS {_quote_if_reserved(alias)}"


def _serialize_order_col(o: Any) -> str:
    """Serialize an ORDER BY column, quoting IRIS reserved-word names."""
    if isinstance(o, SortColumn):
        return o._expr
    order_str = _serialize(o)
    parts = order_str.split(None, 1)
    col_name = _quote_if_reserved(parts[0])
    direction = parts[1] if len(parts) > 1 else ""
    return f"{col_name} {direction}" if direction else col_name


def _resolve_agg(func: str, col_expr: str) -> str:
    if func.lower() not in _AGG_FUNCS:
        raise IrisParkSQLError(f"invalid aggregation function: {func!r}")
    tpl = _UDF_AGG_MAP.get(func.lower())
    if tpl:
        return tpl.format(col_expr)
    return f"{func.upper()}({col_expr})"


class IrisParkSQLError(ValueError):
    pass


def _validate_identifier(label: str, value: str) -> None:
    if not _IDENTIFIER_RE.match(value):
        raise IrisParkSQLError(f"{label}: identificador invalido: {value!r}")


def _validate_filter(condition: str) -> None:
    if not _SAFE_FILTER_RE.match(condition):
        raise IrisParkSQLError(f"Filtro contem caracteres nao permitidos: {condition!r}")


def _validate_order_col(col_spec: str) -> None:
    parts = col_spec.split(None, 1)
    col_name = parts[0]
    _validate_identifier("order column", col_name)
    if len(parts) == 2:
        direction = parts[1].upper()
        if direction not in ("ASC", "DESC"):
            raise IrisParkSQLError(
                f"order column: direcao invalida {direction!r} em {col_spec!r}"
            )


def _validate_limit(value: Any) -> None:
    if not _SAFE_LIMIT_RE.match(str(value)):
        raise IrisParkSQLError(f"LIMIT invalido: {value!r}")


def _format_sql(sql: str) -> str:
    """Format SQL with indentation for readability."""
    # Keywords that start new clauses
    clause_keywords = [
        "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY",
        "LIMIT", "TOP", "UNION ALL", "INNER JOIN", "LEFT JOIN",
        "RIGHT JOIN", "FULL OUTER JOIN", "CROSS JOIN", "LEFT SEMI", "LEFT ANTI",
        "ON", "AND", "OR"
    ]
    result = sql
    for kw in clause_keywords:
        # Add newline before each clause keyword (except at start)
        result = re.sub(
            rf"(?<!^)\b{re.escape(kw)}\b",
            f"\n  {kw}",
            result,
            flags=re.IGNORECASE
        )
    # Indent subqueries
    result = re.sub(r"\)\s+AS\s+\w+", ")\n  AS ", result)
    # Indent JOIN ON conditions
    result = re.sub(r"\s+ON\s+", "\n    ON ", result)
    return result


def _serialize(item: Any) -> str:
    if hasattr(item, "_expr"):
        return item._expr  # type: ignore[attr-defined]
    return str(item)


def _serialize_filter(item: Any) -> str:
    """Return the predicate form when available, otherwise fall back to _expr."""
    predicate = getattr(item, "_predicate", None)
    if predicate is not None:
        return predicate
    return _serialize(item)


def _is_numeric_literal(value: Any) -> bool:
    """Return True if value is an int or float literal (not bool) that may need casting."""
    if isinstance(value, bool):
        return False
    return isinstance(value, (int, float))


def _coalesce_arg_expr(arg: Any, target_type: str) -> str:
    """Render a single COALESCE/IFNULL/NVL argument, casting numeric literals to target_type."""
    if _is_numeric_literal(arg):
        return f"CAST({_quote(arg)} AS {target_type})"
    if isinstance(arg, Column) and _is_numeric_literal_string(arg._expr):
        return f"CAST({arg._expr} AS {target_type})"
    return _serialize(arg)


def _is_numeric_literal_string(expr: str) -> bool:
    """Return True if expr is a bare int or float literal (e.g. '0', '0.0', '-1')."""
    expr = expr.strip()
    if re.fullmatch(r"-?\d+", expr):
        return True
    if re.fullmatch(r"-?\d+\.\d*", expr):
        return True
    return False


_TYPE_CODE_TO_IRIS: dict[int, str] = {
    -6: "TINYINT",
    -5: "BIGINT",
    2: "NUMERIC",
    3: "NUMERIC",
    4: "INTEGER",
    5: "SMALLINT",
    6: "FLOAT",
    7: "REAL",
    8: "DOUBLE",
    12: "VARCHAR(4000)",
    16: "INT",
}


def _iris_type_from_schema_entry(entry: Any) -> str:
    """Normalize a schema type entry to an IRIS SQL type name.

    ``read.py`` stores IRIS type strings such as ``NUMERIC(38,2)``,
    while ``_ensure_schema`` from a JDBC cursor stores integer type codes.
    """
    if isinstance(entry, str):
        return entry
    return _TYPE_CODE_TO_IRIS.get(int(entry), "VARCHAR(4000)")


def _serialize_typed(item: Any, df: IrisDataFrame) -> str:
    """Serialize a Column for SELECT/withColumn, casting numeric literals inside
    COALESCE/IFNULL/NVL when the DataFrame schema is available."""
    func_name = getattr(item, "_coalesce_func", None)
    if func_name is None:
        return _serialize(item)

    schema = getattr(df, "_base_schema", None) or df._ensure_schema()
    if not schema:
        return _serialize(item)
    type_by_name = {
        str(c).lower(): _iris_type_from_schema_entry(t) for c, t in schema
    }

    args: list[Any] = getattr(item, "_coalesce_args", [])
    if not args:
        return _serialize(item)

    # Anchor = first column-reference arg. Its type drives casting of numeric literals.
    anchor_type: str | None = None
    for arg in args:
        if isinstance(arg, str):
            anchor_type = type_by_name.get(arg.lower())
            if anchor_type:
                break
        if isinstance(arg, Column):
            stripped = arg._expr.strip()
            # strip wrapping parentheses, e.g. "(valor)" -> "valor"
            while stripped.startswith("(") and stripped.endswith(")"):
                stripped = stripped[1:-1].strip()
            if _is_identifier(stripped):
                anchor_type = type_by_name.get(stripped.lower())
                if anchor_type:
                    break

    if anchor_type is None:
        return _serialize(item)

    rendered_args = [_coalesce_arg_expr(a, anchor_type) for a in args]
    return f"{func_name}({', '.join(rendered_args)})"


def _is_identifier(value: str) -> bool:
    return _IDENTIFIER_RE.match(value) is not None


def _column_alias(col: Column) -> str:
    expr = col._expr
    name = re.sub(r"[^a-zA-Z0-9_]", "_", expr)
    return f"col_{name[:40]}"


_ANALYTIC_MARKER_RE = re.compile(
    r"^(?P<marker>IRISPARK_(?P<kind>MEDIAN|PERCENTILE|QUANTILE)_ANALYTIC\((?P<col>.+)\))(?:\s+AS\s+(?P<alias>\w+))?$",
    re.IGNORECASE,
)
_ANALYTIC_TOKEN_RE = re.compile(
    r"IRISPARK_(MEDIAN|PERCENTILE|QUANTILE)_ANALYTIC\(", re.IGNORECASE
)


def _analytic_to_udaf(sql: str) -> str:
    """Swap SQL-native analytic markers for the UDAF form (fallback path)."""
    out: list[str] = []
    pos = 0
    while True:
        m = _ANALYTIC_TOKEN_RE.search(sql, pos)
        if not m:
            out.append(sql[pos:])
            break
        depth = 0
        end = m.end() - 1
        i = m.end()
        while i < len(sql):
            ch = sql[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                if depth == 0:
                    end = i
                    break
                depth -= 1
            i += 1
        out.append(sql[pos:m.start()])
        out.append(f"IRISPARK.{m.group(1).upper()}({sql[m.end():end]})")
        pos = end + 1
    return "".join(out)


def _resolve_renamed_col(df: Any, col_name: str) -> str:
    """Resolve alias back to physical column name.

    Returns physical name if aggregation needs it, otherwise returns col_name.
    """
    reversed_renames = {v: k for k, v in df._renamed_cols.items()}
    return reversed_renames.get(col_name, col_name)


class SQLGenerator:
    def __init__(self, dataframe: IrisDataFrame) -> None:
        self.df = dataframe
        self._analytic_ok = False

    def generate(self) -> str:
        self._validate_all()
        self._analytic_ok = (
            not self.df.join_config
            and not self.df._renamed_cols
            and not self.df._drop_duplicates_subset
            and not self.df._union_parts
            and not self.df._unpivot_config
            and not self.df._pivot_col
            and self.df._group_type in (None, "GROUP BY")
        )

        if self.df._union_parts:
            has_post_union_stages = bool(
                self.df.with_columns
                or self.df._renamed_cols
                or self.df._drop_duplicates_subset
                or self.df._unpivot_config
                or self.df._pivot_col
                or self.df.join_config
                or self.df.group_cols
            )
            if not has_post_union_stages:
                return self._generate_union()
            # Post-union transformations: fall through so the union becomes
            # the base subquery (_simple_table_source wraps it) and the extra
            # stages apply on top — matching PySpark DataFrame semantics.

        return self._generate_self()

    def _generate_self(self) -> str:
        joins = self.df.join_config

        if joins and joins[0]["how"] in ("LEFT SEMI", "LEFT ANTI"):
            return self._generate_semi_anti(joins[0])

        # Legacy dedup shortcut applies only to plain dedup pipelines. When
        # an aggregation consumed a composed base (_grouped_base_columns),
        # generation must flow through the grouped-withColumns layering so
        # aggregations compose over the dedup subquery.
        has_consumed_base = bool(getattr(self.df, "_grouped_base_columns", []))

        if (
            self.df._drop_duplicates_subset
            and not self.df.with_columns
            and not has_consumed_base
        ):
            return self._generate_drop_duplicates()

        if self.df.group_cols and self.df._group_type in ("CUBE", "ROLLUP"):
            return self._generate_cube_rollup()

        if self.df._unpivot_config:
            return self._generate_unpivot()

        stack_col = self._find_stack_column()
        if stack_col is not None:
            return self._generate_stack_unpivot(stack_col)

        # withColumns layered over an aggregation: agg() consumes parent
        # stages into _grouped_base_columns, so routing triggers when there
        # are either new post-agg withColumns or consumed base stages.
        grouped_base = getattr(self.df, "_grouped_base_columns", []) or []
        if (
            (self.df.with_columns or grouped_base)
            and (self.df.aggregations or self.df.group_cols)
            and not getattr(self.df, "_in_grouped_wc", False)
        ):
            return self._generate_grouped_with_columns()

        select_clause = self._build_select()
        table_source = self._table_source()

        if self._analytic_ok and _ANALYTIC_TOKEN_RE.search(select_clause):
            parsed = self._parse_analytic_parts()
            if parsed is None:
                select_clause = _analytic_to_udaf(select_clause)
            else:
                return self._generate_analytic(parsed, table_source)

        distinct = "DISTINCT " if self.df._distinct else ""
        top = f"TOP {self.df.limit_n} " if self.df.limit_n is not None else ""
        sql = f"SELECT {distinct}{top}{select_clause} FROM {table_source}"

        where_parts = []
        having_parts = []
        filters_in_subquery = bool(self.df._renamed_cols)
        if self.df.group_cols:
            if not filters_in_subquery:
                agg_aliases = self._get_agg_aliases()
                for f in self.df.filters:
                    filter_str = _serialize_filter(f)
                    if self.df._renamed_cols:
                        reversed_renames = {v: k for k, v in self.df._renamed_cols.items()}
                        for alias, physical in reversed_renames.items():
                            filter_str = re.sub(rf'\b{re.escape(alias)}\b', physical, filter_str)
                    if agg_aliases and any(
                        re.search(rf"\b{re.escape(a)}\b", filter_str)
                        for a in agg_aliases
                    ):
                        having_parts.append(filter_str)
                    else:
                        where_parts.append(filter_str)
        elif not filters_in_subquery:
            for f in self.df.filters:
                filter_str = _serialize_filter(f)
                # Resolve aliases to physical column names in filters
                if self.df._renamed_cols:
                    reversed_renames = {v: k for k, v in self.df._renamed_cols.items()}
                    for alias, physical in reversed_renames.items():
                        filter_str = re.sub(rf'\b{re.escape(alias)}\b', physical, filter_str)
                where_parts.append(filter_str)
        rs = self.df._random_split
        if rs is not None:
            seed, lo, hi = rs
            cond = f"MOD((%ID * 1103515245 + {seed}) * 1103515245 + 12345, 2147483648) / 2147483648.0 >= {lo} AND MOD((%ID * 1103515245 + {seed}) * 1103515245 + 12345, 2147483648) / 2147483648.0 < {hi}"
            where_parts.append(cond)
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)

        if self.df.group_cols:
            group_parts = []
            for g in self.df.group_cols:
                group_str = _serialize(g)
                # Use physical column name in GROUP BY, not alias
                physical_name = _resolve_renamed_col(self.df, group_str)
                group_parts.append(physical_name)
            sql += f" GROUP BY {', '.join(group_parts)}"

        if having_parts:
            sql += " HAVING " + " AND ".join(having_parts)

        # ORDER BY is applied outside subquery
        # When there are renames, use the alias (not physical name) because subquery projects "physical AS alias"
        if self.df.order_cols:
            order_parts = []
            for o in self.df.order_cols:
                if isinstance(o, SortColumn):
                    order_parts.append(o._expr)
                    continue
                order_str = _serialize(o)
                parts = order_str.split(None, 1)
                col_name = _quote_if_reserved(parts[0])
                direction = parts[1] if len(parts) > 1 else ""
                # If col_name is an alias, use it directly (it exists in subquery output)
                # If col_name is physical, keep it
                order_parts.append(f"{col_name} {direction}" if direction else col_name)
            sql += " ORDER BY " + ", ".join(order_parts)

        return sql

    def _generate_grouped_with_columns(self) -> str:
        """withColumns layered around a grouped query, stage-faithfully.

        Derived columns that reference aggregate aliases or use window OVER
        clauses belong ABOVE the grouped query; everything else stays in the
        pre-aggregation pipeline (so expressions over raw rows keep working,
        e.g. computing a month key before ``groupBy``). Chained dependencies
        follow their parent stage.
        """
        # Every entry in with_columns was applied AFTER this aggregation
        # (agg() consumed parent stages into _grouped_base_columns), so all
        # of them layer above the grouped query.
        post_items: list[tuple[str, str]] = []
        for col_name, col_expr in self.df.with_columns:
            expr = self._prepare_withcolumn_expr(
                _serialize_typed(col_expr, self.df),
                resolve_aliases=bool(self.df._renamed_cols),
            )
            post_items.append((col_name, expr))

        # Emit post columns in dependency layers: a column referencing another
        # post column opens a new layer so names are visible when used.
        layers: list[list[tuple[str, str]]] = []
        cur: list[tuple[str, str]] = []
        cur_names: set[str] = set()
        for name, expr in post_items:
            deps_cur = any(
                re.search(rf"\b{re.escape(d)}\b", expr) for d in cur_names
            )
            if deps_cur and cur:
                layers.append(cur)
                cur = []
                cur_names = set()
            cur.append((name, expr))
            cur_names.add(name)
        if cur:
            layers.append(cur)

        grouped_child = self.df._copy(
            with_columns=list(self.df._grouped_base_columns),
            order_cols=[],
            limit_n=None,
        )
        grouped_child._in_grouped_wc = True  # inner generation is the base layer
        grouped_sql = SQLGenerator(grouped_child).generate()
        sql = f"SELECT * FROM ({grouped_sql}) AS _g"
        for i, layer in enumerate(layers):
            frags = ", ".join(f"{e} AS {_quote_if_reserved(n)}" for n, e in layer)
            sql = f"SELECT *, {frags} FROM ({sql}) AS _gc{i + 1}"

        if self.df.order_cols:
            order_parts = []
            for o in self.df.order_cols:
                if isinstance(o, SortColumn):
                    order_parts.append(o._expr)
                    continue
                order_str = _serialize(o)
                parts_o = order_str.split(None, 1)
                cname = _quote_if_reserved(parts_o[0])
                direction = parts_o[1] if len(parts_o) > 1 else ""
                order_parts.append(f"{cname} {direction}" if direction else cname)
            order_sql = " ORDER BY " + ", ".join(order_parts)
        else:
            order_sql = ""

        if self.df.limit_n is not None:
            # TOP plus a visible ORDER BY must stay at the outermost level;
            # IRIS rejects ORDER BY inside a subquery.
            if order_sql:
                return f"SELECT TOP {self.df.limit_n} * FROM ({sql}) AS _gw{order_sql}"
            return f"SELECT TOP {self.df.limit_n} * FROM ({sql}) AS _gw"
        return sql + order_sql

    def _parse_analytic_parts(self) -> list[dict] | None:
        """Parse the select parts into marker specs; None when any part
        carries a marker that cannot be expanded (then callers fall back)."""
        specs: list[dict] = []
        parts = self._build_select_parts()
        if sum(len(_ANALYTIC_TOKEN_RE.findall(p)) for p in parts) != 1:
            return None
        for p in parts:
            m = _ANALYTIC_MARKER_RE.fullmatch(p)
            if not m:
                if _ANALYTIC_TOKEN_RE.search(p):
                    return None
                specs.append({"part": p, "marker": None})
                continue
            marker_text = m.group("marker")
            if len(_ANALYTIC_TOKEN_RE.findall(p)) != 1:
                return None
            kind, col_text = m.group("kind").upper(), m.group("col")
            pct: float | None = None
            if kind == "MEDIAN":
                pct = 0.5
            else:
                pm = re.match(r"(?P<col>.+),\s*(?P<p>[0-9.]+)$", col_text)
                if not pm:
                    return None
                col_text = pm.group("col")
                p_val = float(pm.group("p"))
                if not 0 <= p_val <= 1:
                    return None
                pct = p_val
            specs.append(
                {
                    "part": p,
                    "marker": marker_text,
                    "kind": kind,
                    "col": col_text,
                    "pct": pct,
                    "alias": m.group("alias") or _column_alias(Column(marker_text)),
                }
            )
        return specs

    def _generate_analytic(self, specs: list[dict], table_source: str) -> str:
        """Expand a single SQL-native analytic marker into a nested single-pass
        query (ROW_NUMBER over a partitioned window + linear interpolation)."""
        group_phys = [
            _resolve_renamed_col(self.df, _serialize(g)) for g in self.df.group_cols
        ]
        partition = f"PARTITION BY {', '.join(group_phys)} " if group_phys else ""

        inner_projs: list[str] = []
        outer_parts: list[str] = []
        null_filters: list[str] = []
        for pi, spec in enumerate(specs):
            if spec["marker"] is None:
                outer_parts.append(spec["part"])
                continue
            col, pct = spec["col"], spec["pct"]
            rn = f"_ana_rn{pi}"
            klo = f"_ana_klo{pi}"
            khi = f"_ana_khi{pi}"
            frac = f"_ana_frac{pi}"
            v = f"_ana_v{pi}"
            inner_projs.append(f"{col} AS {v}")
            inner_projs.append(f"ROW_NUMBER() OVER ({partition}ORDER BY {col}) AS {rn}")
            inner_projs.append(
                f"FLOOR((COUNT(*) OVER ({partition}) - 1) * {pct}) + 1 AS {klo}"
            )
            inner_projs.append(
                f"FLOOR((COUNT(*) OVER ({partition}) - 1) * {pct}) + 2 AS {khi}"
            )
            inner_projs.append(
                f"(COUNT(*) OVER ({partition}) - 1) * {pct}"
                f" - FLOOR((COUNT(*) OVER ({partition}) - 1) * {pct}) AS {frac}"
            )
            case = (
                f"MIN(CASE WHEN {rn} = {klo} THEN {v} END) + "
                f"(COALESCE(MIN(CASE WHEN {rn} = {khi} THEN {v} END),"
                f" MIN(CASE WHEN {rn} = {klo} THEN {v} END))"
                f" - MIN(CASE WHEN {rn} = {klo} THEN {v} END)) * MIN({frac})"
            )
            outer_parts.append(f"{case} AS {spec['alias']}")
            null_filters.append(f"({col}) IS NOT NULL")

        agg_aliases = self._get_agg_aliases()
        inner_where: list[str] = []
        having_parts: list[str] = []
        for f in self.df.filters:
            filter_str = _serialize_filter(f)
            if group_phys and agg_aliases and any(
                re.search(rf"\b{re.escape(a)}\b", filter_str) for a in agg_aliases
            ):
                having_parts.append(filter_str)
            else:
                inner_where.append(filter_str)
        rs = self.df._random_split
        if rs is not None:
            seed, lo, hi = rs
            inner_where.append(
                f"MOD((%ID * 1103515245 + {seed}) * 1103515245 + 12345, 2147483648)"
                f" / 2147483648.0 >= {lo} AND MOD((%ID * 1103515245 + {seed})"
                f" * 1103515245 + 12345, 2147483648) / 2147483648.0 < {hi}"
            )
        inner_where.extend(null_filters)

        distinct = "DISTINCT " if self.df._distinct else ""
        top = f"TOP {self.df.limit_n} " if self.df.limit_n is not None else ""
        sql = f"SELECT {distinct}{top}{', '.join(outer_parts)} " \
              f"FROM (SELECT *, {', '.join(inner_projs)} FROM {table_source}"
        if inner_where:
            sql += " WHERE " + " AND ".join(inner_where)
        sql += ") AS _ana"

        if group_phys:
            sql += " GROUP BY " + ", ".join(group_phys)

        if having_parts:
            sql += " HAVING " + " AND ".join(having_parts)

        if self.df.order_cols:
            order_parts = []
            for o in self.df.order_cols:
                if isinstance(o, SortColumn):
                    order_parts.append(o._expr)
                    continue
                order_str = _serialize(o)
                parts = order_str.split(None, 1)
                col_name = _quote_if_reserved(parts[0])
                direction = parts[1] if len(parts) > 1 else ""
                order_parts.append(f"{col_name} {direction}" if direction else col_name)
            sql += " ORDER BY " + ", ".join(order_parts)

        return sql

    def _generate_drop_duplicates(self) -> str:
        self.df._ensure_schema()
        all_cols = [c for c, _ in self.df._schema]
        subset = self.df._drop_duplicates_subset
        partition = ", ".join(subset)
        order = ", ".join(all_cols) + ", %ID"
        select_cols = ", ".join(all_cols)
        inner = (
            f"SELECT {select_cols}, "
            f"ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order}) AS _rn "
            f"FROM {self.df.table_name}"
        )
        top = f"TOP {self.df.limit_n} " if self.df.limit_n is not None else ""
        sql = f"SELECT {top}{select_cols} FROM ({inner}) AS _dedup WHERE _rn = 1"
        if self.df.filters:
            sql += " AND " + " AND ".join(
                _serialize_filter(f) for f in self.df.filters
            )
        if self.df.order_cols:
            sql += " ORDER BY " + ", ".join(
                _serialize_order_col(o) for o in self.df.order_cols
            )
        return sql

    def _generate_union_inner(self) -> str:
        """Bare ``arm UNION ALL arm`` SQL for the union parts only."""
        parts: list[str] = []
        for df in self.df._union_parts:
            sql = SQLGenerator(df).generate()
            # No per-arm parentheses: IRIS rejects a top-level "(SELECT ..)
            # UNION ALL (SELECT ..)" statement (SQLCODE -422), which breaks
            # CTAS/INSERT consumers of the generated SQL.
            parts.append(sql)
        return " UNION ALL ".join(parts)

    def _generate_union(self) -> str:
        inner = self._generate_union_inner()
        has_outer = (
            self.df._distinct
            or self.df.filters
            or self.df.order_cols
            or self.df.limit_n is not None
            or not self._is_all_columns()
        )

        if not has_outer:
            return inner

        distinct = "DISTINCT " if self.df._distinct else ""
        top = f"TOP {self.df.limit_n} " if self.df.limit_n is not None else ""

        if self._is_all_columns() and not self.df._dropped_cols:
            select_clause = "*"
        else:
            select_clause = self._build_select()

        sql = f"SELECT {distinct}{top}{select_clause} FROM ({inner}) AS _u"

        where_parts = [_serialize_filter(f) for f in self.df.filters]
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)

        if self.df.order_cols:
            order_parts = []
            for o in self.df.order_cols:
                if isinstance(o, SortColumn):
                    order_parts.append(o._expr)
                    continue
                order_str = _serialize(o)
                parts = order_str.split(None, 1)
                col_name = _quote_if_reserved(parts[0])
                direction = parts[1] if len(parts) > 1 else ""
                if parts[0] in self.df._renamed_cols:
                    order_parts.append(
                        f"{col_name} AS {_quote_if_reserved(self.df._renamed_cols[parts[0]])}"
                        f"{' ' + direction if direction else ''}"
                    )
                else:
                    order_parts.append(
                        f"{col_name} {direction}" if direction else col_name
                    )
            sql += " ORDER BY " + ", ".join(order_parts)

        return sql

    def _generate_semi_anti(self, join_info: dict[str, Any]) -> str:
        left_source = self._left_table_source()
        right_source = self._right_table_source(join_info)
        left_table = self.df.table_name
        right_table = join_info["right"].table_name
        left_alias = getattr(self.df, '_alias', None) or "l"
        right_alias = getattr(join_info["right"], '_alias', None) or "r"
        on_expr = join_info["on"]._expr
        on_expr = re.sub(rf'\b{re.escape(left_table)}\.', f"{left_alias}.", on_expr)
        on_expr = re.sub(rf'\b{re.escape(right_table)}\.', f"{right_alias}.", on_expr)

        if not left_source.startswith("("):
            left_source = f"SELECT * FROM {left_source}"
            left_source = f"({left_source}) AS {left_alias}"
        elif " AS " not in left_source:
            left_source = f"{left_source} AS {left_alias}"

        if not right_source.startswith("("):
            right_source = f"SELECT * FROM {right_source}"
            right_source = f"({right_source}) AS {right_alias}"
        elif " AS " not in right_source:
            right_source = f"{right_source} AS {right_alias}"

        op = "NOT EXISTS" if join_info["how"] == "LEFT ANTI" else "EXISTS"
        top = f"TOP {self.df.limit_n} " if self.df.limit_n is not None else ""
        sql = (
            f"SELECT {top}* FROM {left_source} "
            f"WHERE {op} (SELECT 1 FROM {right_source} WHERE {on_expr})"
        )

        if self.df.filters:
            sql += " AND " + " AND ".join(
                _serialize_filter(f) for f in self.df.filters
            )

        if self.df.order_cols:
            sql += " ORDER BY " + ", ".join(
                _serialize_order_col(o) for o in self.df.order_cols
            )

        return sql

    def _generate_cube_rollup(self) -> str:
        raw_cols = [_serialize(g) for g in self.df.group_cols]
        n = len(raw_cols)

        subset_indices: list[tuple[int, ...]] = []
        if self.df._group_type == "CUBE":
            for r in range(n + 1):
                for combo in combinations(range(n), r):
                    subset_indices.append(combo)
        else:
            for r in range(n, -1, -1):
                subset_indices.append(tuple(range(r)))

        base = self.df.table_name
        where_clause = ""
        if self.df.filters:
            where_clause = " WHERE " + " AND ".join(_serialize_filter(f) for f in self.df.filters)
        if self.df._sample_fraction is not None and not self.df.with_columns:
            seed = self.df._sample_seed or 0
            cond = f"MOD((%ID * 1103515245 + {seed}) * 1103515245 + 12345, 2147483648) / 2147483648.0 < {self.df._sample_fraction}"
            if where_clause:
                where_clause += f" AND {cond}"
            else:
                where_clause = f" WHERE {cond}"

        parts: list[str] = []
        for subset in subset_indices:
            select_parts: list[str] = []
            for i, col in enumerate(raw_cols):
                if i in subset:
                    select_parts.append(col)
                else:
                    select_parts.append(f"NULL AS {col}")

            for col, func in self.df.aggregations.items():
                if col == "*":
                    select_parts.append(f"{func.upper()}(*)")
                else:
                    alias = f"{func}_{col}"
                    select_parts.append(f"{_resolve_agg(func, col)} AS {alias}")

            if subset:
                gb = ", ".join(raw_cols[i] for i in subset)
                inner = f"SELECT {', '.join(select_parts)} FROM {base}{where_clause} GROUP BY {gb}"
            else:
                inner = f"SELECT {', '.join(select_parts)} FROM {base}{where_clause}"
            parts.append(inner)

        sql = " UNION ALL\n".join(parts)

        if self.df.order_cols:
            order = ", ".join(_serialize_order_col(o) for o in self.df.order_cols)
            sql = f"SELECT * FROM ({sql}) AS _cr ORDER BY {order}"

        if self.df.limit_n is not None:
            sql = f"SELECT TOP {self.df.limit_n} * FROM ({sql}) AS _crl"

        return sql

    def _find_stack_column(self) -> Column | None:
        for c in self.df.select_cols:
            if isinstance(c, Column) and c._expr.upper().startswith("STACK("):
                return c
        return None

    def _generate_stack_unpivot(self, stack_col: Column) -> str:
        pairs = getattr(stack_col, "_stack_pairs", None)
        if not pairs:
            pairs = self._parse_stack_expr(stack_col._expr)
        if not pairs:
            return self._generate_self()

        self.df._ensure_schema()
        schema_names = {c for c, _ in self.df._schema}
        unpivot_cols = [p[1] for p in pairs]
        other_cols = [c for c in schema_names if c not in set(unpivot_cols)]

        base = self._table_source()
        where_clause = ""
        if self.df.filters:
            where_clause = " WHERE " + " AND ".join(_serialize_filter(f) for f in self.df.filters)

        parts: list[str] = []
        for label, col_expr in pairs:
            select_parts: list[str] = []
            select_parts.append(f"{_quote(label)} AS _label")
            select_parts.append(f"{col_expr} AS _value")
            for oc in other_cols:
                select_parts.append(oc)
            parts.append(f"SELECT {', '.join(select_parts)} FROM {base}{where_clause}")

        sql = " UNION ALL\n".join(parts)

        if self.df.order_cols:
            order = ", ".join(_serialize_order_col(o) for o in self.df.order_cols)
            sql = f"SELECT * FROM ({sql}) AS _su ORDER BY {order}"

        if self.df.limit_n is not None:
            sql = f"SELECT TOP {self.df.limit_n} * FROM ({sql}) AS _sul"

        return sql

    def _parse_stack_expr(self, expr: str) -> list[tuple[str, str]] | None:
        m = re.match(r"stack\s*\(\s*(\d+)\s*,\s*(.+?)\)", expr, re.IGNORECASE)
        if not m:
            return None
        n = int(m.group(1))
        rest = m.group(2)
        parts = self._split_stack_args(rest)
        if len(parts) != n * 2:
            return None
        pairs: list[tuple[str, str]] = []
        for i in range(0, len(parts), 2):
            label = parts[i].strip().strip("'\"")
            col_expr = parts[i + 1].strip()
            pairs.append((label, col_expr))
        return pairs

    def _split_stack_args(self, s: str) -> list[str]:
        parts: list[str] = []
        current: list[str] = []
        depth = 0
        in_string = False
        string_char = ""
        for ch in s:
            if in_string:
                current.append(ch)
                if ch == string_char:
                    in_string = False
                continue
            if ch in ("'", '"'):
                in_string = True
                string_char = ch
                current.append(ch)
                continue
            if ch == "(":
                depth += 1
                current.append(ch)
            elif ch == ")":
                depth -= 1
                current.append(ch)
            elif ch == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            parts.append("".join(current).strip())
        return parts

    def _generate_unpivot(self) -> str:
        cfg = self.df._unpivot_config
        assert cfg is not None, "unpivot requires an unpivot_config"
        label_col = cfg["label_col"]
        value_col = cfg["value_col"]
        cols = cfg["cols"]

        self.df._ensure_schema()
        schema_names = {c for c, _ in self.df._schema}
        other_cols = [c for c in schema_names if c not in set(cols)]

        base = self._table_source()
        where_clause = ""
        if self.df.filters:
            where_clause = " WHERE " + " AND ".join(_serialize_filter(f) for f in self.df.filters)

        qualified_cols = {c: c for c in cols}
        qualified_other = {oc: oc for oc in other_cols}
        if self.df.join_config:
            left_schema = getattr(self.df, "_left_schema", None)
            right_schema_entries = getattr(self.df, "_right_schema", [])
            right_suffix = getattr(self.df, "_right_suffix", {})
            left_alias = getattr(self.df, '_alias', None) or "l"
            if left_schema is not None:
                left_names = {c.lower(): c for c, _ in left_schema}
                right_names_map: dict[str, tuple[str, str]] = {}
                for alias, r_cols in right_schema_entries:
                    for c, _ in r_cols:
                        orig = right_suffix.get(c, c)
                        right_names_map[c.lower()] = (alias, orig)
                for c in cols:
                    cl = c.lower()
                    if cl in left_names:
                        qualified_cols[c] = f"{left_alias}.{left_names[cl]}"
                    elif cl in right_names_map:
                        a, orig = right_names_map[cl]
                        qualified_cols[c] = f"{a}.{orig}"
                for oc in other_cols:
                    ocl = oc.lower()
                    if ocl in left_names:
                        qualified_other[oc] = f"{left_alias}.{left_names[ocl]}"
                    elif ocl in right_names_map:
                        a, orig = right_names_map[ocl]
                        qualified_other[oc] = f"{a}.{orig}"
                    else:
                        qualified_other[oc] = oc

        parts: list[str] = []
        for c in cols:
            select_parts: list[str] = []
            select_parts.append(f"'{c}' AS {_quote_if_reserved(label_col)}")
            select_parts.append(f"{qualified_cols[c]} AS {_quote_if_reserved(value_col)}")
            for oc in other_cols:
                select_parts.append(qualified_other[oc])
            parts.append(f"SELECT {', '.join(select_parts)} FROM {base}{where_clause}")

        sql = " UNION ALL\n".join(parts)

        if self.df.order_cols:
            order = ", ".join(_serialize_order_col(o) for o in self.df.order_cols)
            sql = f"SELECT * FROM ({sql}) AS _up ORDER BY {order}"

        if self.df.limit_n is not None:
            sql = f"SELECT TOP {self.df.limit_n} * FROM ({sql}) AS _upl"

        return sql

    def _table_source(self) -> str:
        if self.df.join_config:
            return self._build_join_chain(self.df.join_config)
        return self._simple_table_source()

    def _simple_table_source(self) -> str:
        base = self.df.table_name

        if self.df._union_parts:
            # Post-union stages: the union result becomes the row source.
            base = f"({self._generate_union_inner()}) AS _u"

        if self.df._drop_duplicates_subset:
            # Dedup becomes a subquery so later stages (withColumns, filters)
            # keep working on top of it instead of being bypassed. Physical
            # columns of the BASE relation only: a projected child carries a
            # narrowed _schema (selected columns), so prefer the locked
            # _base_schema before falling back to _ensure_schema.
            dedup_derived = {name for name, _ in self.df.with_columns}
            base_schema = (
                getattr(self.df, "_base_schema", None) or self.df._ensure_schema()
            )
            all_cols = [
                c for c, _ in base_schema if c not in dedup_derived
            ]
            partition = ", ".join(
                c for c in self.df._drop_duplicates_subset if c not in dedup_derived
            )
            # %ID is not visible across a UNION alias; exact-duplicate rows
            # make tie-order irrelevant there, so it is safe to omit.
            id_tail = "" if self.df._union_parts else ", %ID"
            order = ", ".join(all_cols) + id_tail
            sel_cols = ", ".join(all_cols)
            dedup_sql = (
                f"SELECT {sel_cols} FROM ("
                f"SELECT {sel_cols}, "
                f"ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order}) AS _rn "
                f"FROM {base}"
                f") AS _dd1 WHERE _rn = 1"
            )
            base = f"({dedup_sql}) AS _dd"

        if self.df._sample_fraction is not None and not self.df.join_config:
            seed = self.df._sample_seed or 0
            cond = f"MOD((%ID * 1103515245 + {seed}) * 1103515245 + 12345, 2147483648) / 2147483648.0 < {self.df._sample_fraction}"
            sample_clause = f" WHERE {cond}"
        else:
            sample_clause = ""

        if self.df.with_columns:
            merged_parts = self._base_select_parts()
            derived_names: set[str] = set()
            sequential: list[tuple[str, Column]] = []
            for col_name, col_expr in self.df.with_columns:
                expr_str = self._prepare_withcolumn_expr(
                    _serialize_typed(col_expr, self.df), resolve_aliases=bool(self.df._renamed_cols)
                )
                # If this expression references a prior derived column, it must be
                # evaluated in a separate subquery on top of the merged projection.
                if any(re.search(rf"\b{re.escape(dn)}\b", expr_str) for dn in derived_names):
                    sequential.append((col_name, col_expr))
                else:
                    merged_parts.append(f"{expr_str} AS {_quote_if_reserved(col_name)}")
                derived_names.add(col_name)
            # When renames are present, filters may reference aliases; apply them
            # inside the merged subquery so alias-to-physical resolution works.
            where_clause = ""
            if self.df._renamed_cols:
                where_clause = self._filters_where_clause(resolve_aliases=True)
            result = f"(SELECT {', '.join(merged_parts)} FROM {base}{sample_clause}{where_clause}) AS _wc0"
            for i, (col_name, col_expr) in enumerate(sequential):
                expr_str = self._prepare_withcolumn_expr(
                    _serialize_typed(col_expr, self.df), resolve_aliases=False
                )
                result = f"(SELECT *, {expr_str} AS {_quote_if_reserved(col_name)} FROM {result}) AS _wc{i + 1}"
            return result

        if self.df._renamed_cols:
            self.df._ensure_schema()
            select_parts = self._renamed_select_parts()
            # Apply filters inside subquery when there are renames, so that alias
            # references in filters resolve against the rename projection.
            where_clause = self._filters_where_clause(
                resolve_aliases=bool(self.df._renamed_cols)
            )
            return f"(SELECT {', '.join(select_parts)} FROM {base}{sample_clause}{where_clause}) AS _ren"

        if sample_clause:
            return f"(SELECT * FROM {base}{sample_clause}) AS _sample"
        return base

    def _left_table_source(self) -> str:
        alias = getattr(self.df, '_alias', None) or "l"
        base = self.df.table_name

        if self.df.with_columns:
            merged_parts = self._base_select_parts()
            derived_names: set[str] = set()
            sequential: list[tuple[str, Column]] = []
            for col_name, col_expr in self.df.with_columns:
                expr_str = self._prepare_withcolumn_expr(
                    _serialize(col_expr), resolve_aliases=bool(self.df._renamed_cols)
                )
                if any(re.search(rf"\b{re.escape(dn)}\b", expr_str) for dn in derived_names):
                    sequential.append((col_name, col_expr))
                else:
                    merged_parts.append(f"{expr_str} AS {_quote_if_reserved(col_name)}")
                derived_names.add(col_name)
            result = f"(SELECT {', '.join(merged_parts)} FROM {base}) AS _wc0"
            for i, (col_name, col_expr) in enumerate(sequential):
                expr_str = self._prepare_withcolumn_expr(
                    _serialize_typed(col_expr, self.df), resolve_aliases=False
                )
                result = f"(SELECT *, {expr_str} AS {_quote_if_reserved(col_name)} FROM {result}) AS _wc{i + 1}"
            return self._apply_join_alias(result, alias)

        if self.df._renamed_cols:
            self.df._ensure_schema()
            select_parts = self._renamed_select_parts()
            return f"(SELECT {', '.join(select_parts)} FROM {base}) AS {alias}"

        return f"{base} AS {alias}"

    def _base_select_parts(self) -> list[str]:
        """Return SELECT parts for the base table, applying renames if any.

        Used when merging renames and withColumns into a single subquery so
        column expressions can reference physical names in the same scope.
        Derived withColumn columns are excluded because they are added by
        expressions below, not read from the base table.
        """
        if self.df._renamed_cols:
            return self._renamed_select_parts()
        return ["*"]

    def _renamed_select_parts(self) -> list[str]:
        """Return SELECT parts projecting physical columns with aliases."""
        self.df._ensure_schema()
        derived = {name for name, _ in self.df.with_columns}
        parts = []
        for c, _ in self.df._schema:
            if c in derived:
                continue
            if c in self.df._renamed_cols:
                parts.append(f"{c} AS {_quote_if_reserved(self.df._renamed_cols[c])}")
            else:
                parts.append(c)
        return parts

    def _prepare_withcolumn_expr(self, expr_str: str, resolve_aliases: bool) -> str:
        """Normalize a withColumn expression for embedding in a subquery.

        - Strip the base table qualifier (e.g. ``vendas.valor`` -> ``valor``)
          because the base table is wrapped in a subquery alias.
        - Optionally resolve renamed aliases back to physical names; this is
          needed in the merged subquery where expressions are evaluated
          directly against ``FROM {base}``.
        """
        expr_str = re.sub(rf"\b{re.escape(self.df.table_name)}\.", "", expr_str)
        if resolve_aliases and self.df._renamed_cols:
            reversed_renames = {v: k for k, v in self.df._renamed_cols.items()}
            for alias, physical in reversed_renames.items():
                expr_str = re.sub(rf"\b{re.escape(alias)}\b", physical, expr_str)
        return expr_str

    def _filters_where_clause(self, resolve_aliases: bool = False) -> str:
        """Build a WHERE clause from filters, optionally resolving aliases."""
        if not self.df.filters:
            return ""
        where_parts = []
        for f in self.df.filters:
            filter_str = _serialize_filter(f)
            if resolve_aliases and self.df._renamed_cols:
                reversed_renames = {v: k for k, v in self.df._renamed_cols.items()}
                for alias, physical in reversed_renames.items():
                    filter_str = re.sub(rf"\b{re.escape(alias)}\b", physical, filter_str)
            where_parts.append(filter_str)
        return " WHERE " + " AND ".join(where_parts)

    @staticmethod
    def _apply_join_alias(source: str, alias: str) -> str:
        """Replace the trailing alias of a subquery with the join correlation name."""
        m = re.search(r"\)\s+AS\s+\w+\s*$", source)
        if m:
            return source[:m.start()] + f") AS {alias}"
        return f"{source} AS {alias}"

    def _right_table_source(self, join_info: dict[str, Any], alias: str = "r") -> str:
        right = join_info["right"]
        right_alias = getattr(right, '_alias', None) or alias
        right_gen = SQLGenerator(right)
        raw = right_gen._simple_table_source()
        return self._apply_join_alias(raw, right_alias)

    def _build_join_chain(self, joins: list[dict[str, Any]]) -> str:
        left_source = self._left_table_source()
        multi = len(joins) > 1
        left_alias = getattr(self.df, '_alias', None) or "l"
        alias_map: dict[str, str] = {self.df.table_name: left_alias}
        for i, link in enumerate(joins):
            right_df = link["right"]
            right_alias = getattr(right_df, '_alias', None) or (f"r{i}" if multi else "r")
            right_source = self._right_table_source(link, right_alias)
            join_type = link["how"]
            right_table = right_df.table_name
            self_join = right_table in alias_map
            alias_map[right_table] = right_alias
            if "on" not in link:
                left_source = f"{left_source} {join_type} {right_source}"
                continue
            on_expr = link["on"]._expr
            if self_join:
                on_expr = re.sub(
                    rf"\b{re.escape(right_table)}\.", f"{left_alias}.", on_expr, count=1
                )
                on_expr = re.sub(
                    rf"\b{re.escape(right_table)}\.", f"{right_alias}.", on_expr, count=1
                )
            else:
                for tbl, alias in alias_map.items():
                    on_expr = re.sub(rf"\b{re.escape(tbl)}\.", f"{alias}.", on_expr)
            left_source = f"{left_source} {join_type} {right_source} ON {on_expr}"
        return left_source

    def _validate_all(self) -> None:
        _validate_identifier("table_name", self.df.table_name)

        for col in self.df.select_cols:
            if isinstance(col, Column):
                continue
            if col == "*":
                continue
            _validate_identifier("select column", col)

        for condition in self.df.filters:
            if isinstance(condition, Column):
                _validate_filter(condition._expr)
            else:
                _validate_filter(condition)

        for col in self.df.group_cols:
            if isinstance(col, Column):
                continue
            _validate_identifier("group column", col)

        for col in self.df.aggregations:
            if isinstance(col, Column):
                continue
            if col != "*":
                _validate_identifier("aggregation column", col)

        for col_name, _ in self.df.with_columns:
            _validate_identifier("withColumn name", col_name)

        for old_name, new_name in self.df._renamed_cols.items():
            _validate_identifier("withColumnRenamed existing", old_name)
            _validate_identifier("withColumnRenamed new", new_name)

        if self.df._unpivot_config:
            cfg = self.df._unpivot_config
            _validate_identifier("unpivot label_col", cfg["label_col"])
            _validate_identifier("unpivot value_col", cfg["value_col"])
            for c in cfg["cols"]:
                _validate_identifier("unpivot column", c)

        for col in self.df.order_cols:
            if isinstance(col, Column):
                continue
            _validate_order_col(col)

        if self.df.limit_n is not None:
            _validate_limit(self.df.limit_n)

        if self.df.join_config:
            for link in self.df.join_config:
                _validate_identifier(
                    "join right table", link["right"].table_name
                )

        if self.df._pivot_col:
            _validate_identifier("pivot column", self.df._pivot_col)
            if not self.df._pivot_values:
                raise IrisParkSQLError("pivot values list is empty")
            for v in self.df._pivot_values:
                if not str(v).strip():
                    raise IrisParkSQLError("empty pivot value")

        if self.df._sample_fraction is not None:
            if not 0.0 <= self.df._sample_fraction <= 1.0:
                raise IrisParkSQLError(
                    f"sample fraction must be between 0 and 1, got {self.df._sample_fraction}"
                )

        if self.df._group_type not in ("GROUP BY", "CUBE", "ROLLUP"):
            raise IrisParkSQLError(f"invalid group_type: {self.df._group_type}")

    def _is_all_columns(self) -> bool:
        return (
            len(self.df.select_cols) == 1
            and isinstance(self.df.select_cols[0], str)
            and self.df.select_cols[0] == "*"
        )

    def _get_agg_aliases(self) -> set[str]:
        aliases: set[str] = set()
        for col, func in self.df.aggregations.items():
            aliases.add(f"{func}_{col}")
        for c in self.df._agg_exprs:
            if hasattr(c, "_expr"):
                m = re.search(r"\bAS\s+(\w+)\s*$", c._expr, re.IGNORECASE)
                if m:
                    aliases.add(m.group(1))
        return aliases

    def _build_select(self) -> str:
        return ", ".join(self._build_select_parts())

    def _build_select_parts(self) -> list[str]:
        if self.df._pivot_col and self.df._pivot_values:
            return [self._build_pivot_select()]

        if self.df.aggregations:
            parts: list[str] = []

            for col in self.df.group_cols:
                col_str = _serialize(col)
                if col_str in self.df._renamed_cols:
                    parts.append(f"{col_str} AS {_quote_if_reserved(self.df._renamed_cols[col_str])}")
                else:
                    parts.append(col_str)

            for col, func in self.df.aggregations.items():
                if col == "*":
                    if func.lower() == "count":
                        parts.append('COUNT(*) AS "count"')
                    else:
                        parts.append(f"{func.upper()}(*)")
                    continue
                # If col is an alias (renamed column), use the alias in aggregation
                # because the subquery projects "physical AS alias"
                if self.df._renamed_cols and col in self.df._renamed_cols.values():
                    # col is an alias, use it directly in aggregation
                    agg_col = col
                else:
                    # col is a physical name, resolve if needed
                    agg_col = _resolve_renamed_col(self.df, col)
                alias = f"{func}_{col}"
                parts.append(f"{_resolve_agg(func, agg_col)} AS {_quote_if_reserved(alias)}")

            return parts

        if self.df._agg_exprs:
            parts = [_serialize(c) for c in self.df.group_cols]
            for c in self.df._agg_exprs:
                s = _serialize(c)
                if isinstance(c, Column):
                    if " AS " in s:
                        s = _quote_expr_alias(s)
                    else:
                        s = f"{s} AS {_column_alias(c)}"
                parts.append(s)
            if not self._analytic_ok:
                parts = [_analytic_to_udaf(p) for p in parts]
            return parts

        if self.df._dropped_cols:
            self.df._ensure_schema()
            dropped_lower = set(c.lower() for c in self.df._dropped_cols)
            cols = self._get_all_column_names()
            select_cols = [c for c in cols if c.lower() not in dropped_lower]
            if select_cols:
                if self.df._renamed_cols:
                    renamed_cols = {v: k for k, v in self.df._renamed_cols.items()}
                    select_cols = [renamed_cols.get(c, c) for c in select_cols]
                return select_cols

        if self._is_all_columns():
            if self.df.join_config:
                self.df._ensure_schema()
                left_cols = getattr(self.df, "_left_schema", None)
                if left_cols is not None:
                    left_alias = getattr(self.df, '_alias', None) or "l"
                    parts = [f"{left_alias}.{c}" for c, _ in left_cols]
                    right_suffix = getattr(self.df, "_right_suffix", {})
                    for alias, right_cols in self.df._right_schema:
                        for rc, _ in right_cols:
                            orig = right_suffix.get(rc, rc)
                            if orig != rc:
                                parts.append(f"{alias}.{orig} AS {_quote_if_reserved(rc)}")
                            else:
                                parts.append(f"{alias}.{rc}")
                    return parts
            if self.df._renamed_cols:
                return ["*"]
            if self.df._fillna_values or getattr(self.df, "_replace_values", None):
                self.df._ensure_schema()
                cols = self._get_all_column_names()
                return self._apply_renames_and_fillna(cols)
            return ["*"]

        parts = []
        for c in self.df.select_cols:
            s = _serialize_typed(c, self.df) if isinstance(c, Column) else _serialize(c)
            if isinstance(c, Column):
                if " AS " in s:
                    parts.append(_quote_expr_alias(s))
                else:
                    alias = _column_alias(c)
                    parts.append(f"{s} AS {alias}")
            else:
                # c is a string - check if it's a renamed column
                if self.df._renamed_cols and c in self.df._renamed_cols.values():
                    # c is an alias, use it directly
                    parts.append(c)
                else:
                    parts.append(s)
        if not self._analytic_ok:
            parts = [_analytic_to_udaf(p) for p in parts]
        return parts

    def _build_pivot_select(self) -> str:
        parts: list[str] = []
        for col in self.df.group_cols:
            parts.append(_serialize(col))
        pivot_col = self.df._pivot_col
        for col, func in self.df.aggregations.items():
            # pivot().count() records aggregations as {"*": "count"}; inside a
            # CASE the placeholder must be a literal 1 (THEN * is invalid SQL),
            # matching COUNT(*) row-counting semantics.
            base = "1" if (col == "*" and func.lower() == "count") else col
            for val in self.df._pivot_values or []:
                sval = str(val)
                alias = re.sub(r"[^a-zA-Z0-9_]", "_", sval) or "_"
                if alias[0].isdigit():
                    # Digit-leading identifiers are invalid unquoted; quoting
                    # preserves the readable header ("18_29").
                    alias = f'"{alias}"'
                quoted = _quote(sval)
                col_expr = f"CASE WHEN {pivot_col} = {quoted} THEN {base} END"
                parts.append(f"{_resolve_agg(func, col_expr)} AS {alias}")
        return ", ".join(parts)

    def _apply_renames_and_fillna(self, cols: list[str]) -> list[str]:
        renamed = self.df._renamed_cols
        fillna = self.df._fillna_values
        replaces = getattr(self.df, "_replace_values", None) or {}
        parts: list[str] = []
        for c in cols:
            expr = c
            if c in replaces:
                whens = " ".join(
                    f"WHEN {c} = {_quote(o)} THEN {_quote(n)}" for o, n in replaces[c]
                )
                expr = f"CASE {whens} ELSE {expr} END"
            if c in fillna:
                val = fillna[c]
                expr = f"COALESCE({expr}, {_quote(val)})"
            if c in renamed:
                parts.append(f"{expr} AS {_quote_if_reserved(renamed[c])}")
            elif c in fillna or c in replaces:
                parts.append(f"{expr} AS {_quote_if_reserved(c)}")
            else:
                parts.append(expr)
        return parts

    def _get_all_column_names(self) -> list[str]:
        schema = getattr(self.df, "_schema", None)
        if schema:
            return [c for c, _ in schema]
        return ["*"]
