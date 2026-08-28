# Project Patterns & Conventions

> Extracted rules (Fa — Model) distilled from `LESSONS_LEARNED.md` and the empirical
> engineering docs. Per `AGENTS.md`, a rule here has **priority over analogies (Ma)** — treat
> it as the canonical model. Each rule cites its origin and a canonical example.

---

## A. Execution Engine Selection

### Rule 1: SQL-first engine selection
- **Origin**: `rules_functions_aggregations.md` §2/§26; `LESSONS_LEARNED.md` (corr/median).
- **Context**: Choosing how to implement any operation.
- **Mandate**: Prefer **Native SQL → ObjectScript UDF → ObjectScript UDAF → Embedded Python**.
  Never create an ObjectScript/Python function when native IRIS SQL can represent the operation.
  Python is an analytical extension, not the default execution engine.
- **Canonical Example (Fa)**:
  ```python
  df.select(avg("salary"))   # → SELECT AVG(salary) FROM table_name
  ```

### Rule 2: Move computation to the data
- **Origin**: `rules_functions_aggregations.md` §24.
- **Context**: Any operation that can run inside IRIS SQL/ObjectScript.
- **Mandate**: Keep computation close to the data; transfer large datasets to Python only when
  the workload genuinely requires it.
- **Canonical Example (Fa)**: `df.stat.corr("x", "y")` computes inside IRIS, never loading the
  full dataset into Python.

---

## B. UDAF Authoring

### Rule 3: `MERGE WITH` is mandatory for `%PARALLEL`
- **Origin**: `rules_functions_aggregations.md` §6.2(1); `LESSONS_LEARNED.md` (corr/moments).
- **Context**: Any UDAF intended to behave like a Spark primitive.
- **Mandate**: Always ship a merge function. Without it IRIS *silently* refuses `%PARALLEL`
  (runs single-threaded). Chan's parallel combination is the canonical choice.
- **Canonical Example (Fa)**: `IRISPARK.CORR` Welford/Chan state with `MERGE WITH`; `%PARALLEL`
  ≡ serial verified on every dataset shape.

### Rule 4: Online algorithms over Σ-formulas
- **Origin**: `rules_functions_aggregations.md` §6.2(2); `LESSONS_LEARNED.md` (corr `inf`).
- **Context**: Any statistical aggregate (corr, covariance, variance, stddev, moments).
- **Mandate**: Never arrange state so the answer is a difference of two huge near-equal
  quantities. Use centered/online algorithms (Welford/Chan, Pébay).
- **Canonical Example (Fa)**: `IRISPARK.CORR` state `n|mx|my|C|m2x|m2y`; stable at 1e9 offsets
  where the Σ-formula returned `inf`.

### Rule 5: ObjectScript has no operator precedence — parenthesize
- **Origin**: `rules_functions_aggregations.md` §6.2(3).
- **Context**: Any non-trivial arithmetic in an ObjectScript body.
- **Mandate**: Evaluate left to right; parenthesize every expression exactly as intended
  (`mx + ($DOUBLE(dx) / $DOUBLE(n))`).
- **Canonical Example (Fa)**:
  ```objectscript
  Set mx = mx + ($DOUBLE(dx) / $DOUBLE(n))
  ```

### Rule 6: `NEW` every local
- **Origin**: `rules_functions_aggregations.md` §6.2(4).
- **Context**: `LANGUAGE OBJECTSCRIPT` function/aggregate bodies.
- **Mandate**: Declare `NEW` for every local; otherwise locals leak across rows/invocations
  (silent state corruption under parallel/nested execution).
- **Canonical Example (Fa)**: Every `IRISPARK.*` helper declares its locals with `NEW`.

### Rule 7: Qualify every helper as `IRISPARK.*`
- **Origin**: `rules_functions_aggregations.md` §6.2(5).
- **Context**: Helper functions referenced by an aggregate.
- **Mandate**: Use `CREATE OR REPLACE FUNCTION IRISPARK.<name>`; an unqualified helper lands in
  the caller's schema and fails at *execution* time, not DDL time.
- **Canonical Example (Fa)**: `IRISPARK.corr_final`, `IRISPARK.mom_merge`, `IRISPARK.max_by_merge`.

### Rule 8: SQL NULL arrives as `""` — test before `$DOUBLE`
- **Origin**: `rules_functions_aggregations.md` §6.2(6).
- **Context**: Any ObjectScript body receiving SQL values.
- **Mandate**: Guard `x = ""` before any numeric conversion (`$DOUBLE("")` is `0`). For
  aggregates, skip incomplete pairs (pairwise-complete semantics).
- **Canonical Example (Fa)**:
  ```objectscript
  If x = "" Quit state   ; skip NULL pair, carry state unchanged
  ```

### Rule 9: DDL mechanics — idempotent, explicit types, no reserved words
- **Origin**: `rules_functions_aggregations.md` §6.2(7); `LESSONS_LEARNED.md` (AGG_FIRST).
- **Context**: Creating/updating aggregates.
- **Mandate**: Use `CREATE OR REPLACE AGGREGATE` (no DROP + exception-swallowing); specify
  `RETURNS DOUBLE` explicitly; avoid reserved-word names (`FIRST` → `AGG_FIRST`).
- **Canonical Example (Fa)**:
  ```sql
  CREATE OR REPLACE AGGREGATE IRISPARK.AGG_FIRST(x) RETURNS DOUBLE { ... }
  ```

### Rule 10: `%PARALLEL` hint placement: `FROM %PARALLEL <table>`
- **Origin**: `rules_functions_aggregations.md` §6.2(8); `LESSONS_LEARNED.md`.
- **Context**: Any parallel-aggregation verification.
- **Mandate**: Write `FROM %PARALLEL <table>`; the post-table position is a parse error on 2026.2.
- **Canonical Example (Fa)**:
  ```sql
  SELECT IRISPARK.CORR(x, y) FROM %PARALLEL vendas
  ```

### Rule 11: State representation is the perf story
- **Origin**: `rules_functions_aggregations.md` §6.2(9); `LESSONS_LEARNED.md` (MAXSTRING).
- **Context**: Choosing UDAF state encoding.
- **Mandate**: Prefer fixed-size state (moments) or `$LIST` over pipe-separated `VARCHAR`.
  Mind size limits: `VARCHAR(4000)` ≈ 8k values/group; past ~250k values/group the concat
  exceeds `MAXSTRING` and dies FATALLY (SQLCODE -400). Use the SQL-native analytic engine for
  large groups.
- **Canonical Example (Fa)**: `IRISPARK.SKEWNESS` fixed-size `n|mean|M2|M3|M4`; median uses the
  analytic engine at scale.

### Rule 12: `While` conditions — bare vars/literals only
- **Origin**: `rules_functions_aggregations.md` §6.1; `LESSONS_LEARNED.md` (100% CPU wedge).
- **Context**: Any `While` loop in a SQL-embedded ObjectScript body.
- **Mandate**: Hoist arithmetic into a `Set` before the loop or parenthesize it; an
  unparenthesized expression (`While i <= n + 1`) wedges a worker at 100% CPU forever.
- **Canonical Example (Fa)**:
  ```objectscript
  Set m = n + 1
  While i <= m { ... }
  ```

### Rule 13: Prove rewrites with discriminating tests
- **Origin**: `rules_functions_aggregations.md` §6.2(10); `LESSONS_LEARNED.md` (large_offset).
- **Context**: Any UDAF/UDF rewrite.
- **Mandate**: A rewrite is only provable if a test exists that the old implementation fails.
  Standard matrix: pandas parity across shapes, NULL-pair skipping, constant → NULL,
  `n < 2` → NULL, empty → NULL, `%PARALLEL` ≡ serial, plus one magnitude-stress case.
- **Canonical Example (Fa)**: the `large_offset` (1e9 base) dataset caught the corr `inf` bug.

---

## C. SQL Generation

### Rule 14: Use `POWER()` not `^`/`**` in aggregate expressions
- **Origin**: `CHANGELOG.md` (v1.1 corr fix); `LESSONS_LEARNED.md`.
- **Context**: Exponentiation inside aggregate SQL.
- **Mandate**: IRIS rejects both `^` and `**` as exponentiation in aggregate expressions
  (SQLCODE -1 / -12). Use `POWER(expr, n)`.
- **Canonical Example (Fa)**:
  ```sql
  SELECT POWER(SUM(x), 2) FROM t
  ```

### Rule 15: Quote reserved words in identifiers and ORDER BY
- **Origin**: `CHANGELOG.md` (v1.1 count alias); `LESSONS_LEARNED.md`; blocklist synced
  against the official IRIS 2026.2 reserved-word list (RSQL_reservedwords).
- **Context**: Any identifier that collides with an IRIS reserved word (e.g. `count`).
- **Mandate**: Double-quote reserved-word identifiers (`"count"`) and ORDER BY references
  (`"count" DESC`); IRIS uppercases unquoted identifiers then rejects them.
- **Choke-point mandate**: Every identifier emission point (`AS <alias>`, ORDER BY refs,
  rename targets, unpivot/pivot aliases) must route through
  `sql_generator._quote_if_reserved`. Do NOT default-quote all identifiers: quoted
  identifiers are case-sensitive in IRIS, while unquoted ones resolve case-insensitively —
  blanket quoting breaks matching against pre-existing tables. The regression net in
  `tests/test_reserved_word_scan.py` must stay green.
- **Canonical Example (Fa)**:
  ```sql
  SELECT COUNT(*) AS "count" FROM t ORDER BY "count" DESC
  ```

### Rule 16: `COALESCE` over `IFNULL`
- **Origin**: `CHANGELOG.md` (v1.1 ifnull fix).
- **Context**: Null-replacement in string-context queries.
- **Mandate**: IRIS `IFNULL` returns NULL for non-null values in string context; use
  two-argument `COALESCE(a, b)`.
- **Canonical Example (Fa)**:
  ```sql
  SELECT COALESCE(valor, 0.0) FROM t
  ```

### Rule 17: `isnull`/`isnotnull` are context-sensitive
- **Origin**: `CHANGELOG.md` (v1.1 isnull fix).
- **Context**: Rendering null checks.
- **Mandate**: Scalar form `(CASE WHEN col IS NULL THEN 1 ELSE 0 END)` in SELECT/withColumn;
  boolean predicate form `(col IS NULL)` in WHERE/HAVING.
- **Canonical Example (Fa)**: `when(col("c").isNull(), 1)` → `CASE WHEN (c IS NULL) THEN 1 ELSE 0 END`.

### Rule 18: Pairwise-complete null gating for corr/covar
- **Origin**: `CHANGELOG.md` (v1.1 corr); `rules_functions_aggregations.md`.
- **Context**: Correlation/covariance over data with NULLs.
- **Mandate**: Gate every term on `x IS NOT NULL AND y IS NOT NULL` via `SUM(CASE WHEN ...)`;
  native `SUM` skips NULLs but `COUNT(*)` would not, mis-counting `n`.
- **Canonical Example (Fa)**:
  ```sql
  SELECT SUM(CASE WHEN x IS NOT NULL AND y IS NOT NULL THEN x*y END) ...
  ```

### Rule 19: UDF call sites render lowercase
- **Origin**: `CHANGELOG.md` (edge-case parity); `LESSONS_LEARNED.md`.
- **Context**: Schema-qualified UDF references.
- **Mandate**: IRIS resolves unqualified UDF names case-insensitively but schema-qualified
  lookups are exact-case (intermittent SQLCODE -359). Render UDF call sites lowercase.
- **Canonical Example (Fa)**: `regexp_extract`, `irispark_regexp_replace`, `irispark_split`, `md5`.

### Rule 20: Guard domain errors → NULL
- **Origin**: `CHANGELOG.md` (edge-case parity); `LESSONS_LEARNED.md` (pow, regexp).
- **Context**: Functions whose inputs can produce Inf/NaN or no-match.
- **Mandate**: Guard domain-error inputs to NULL (IRIS cannot represent Inf/NaN; `POWER(0,-1)`
  is a fatal SQLCODE -400). `regexp_extract` no-match → NULL (IRIS VARCHAR maps `''` to NULL).
- **Canonical Example (Fa)**: `pow`/`power` guarded to NULL; `regexp_extract` null-guards input.

### Rule 20a: Scalar functions render column args via `_to_col_expr`, not `_to_sql_expr`
- **Origin**: `CHANGELOG.md` (2026-08-21 scalar-functions fix).
- **Context**: Any scalar function whose argument is a column reference (math, string, date,
  hash, trig, conditional, aggregate, window).
- **Mandate**: Render the **column** argument via `_to_col_expr()` (bare identifier); reserve
  `_to_sql_expr()` for **literal** arguments (scale, pattern, replacement, offset, base).
  Using `_to_sql_expr()` on a column quotes it as a string literal: `log("x")` → `LOG('x')`
  (fatal `SQLCODE -400 ILLEGAL VALUE`), `sqrt("x")` → `SQRT('x')` (coerced to 0),
  `upper("nome")` → the literal `'nome'` (wrong result). `_to_col_expr()` is the PySpark
  convention (`sqrt("x")` = column reference).
- **Canonical Example (Fa)**:
  ```python
  def sqrt(col_ref): return Column(f"SQRT({_to_col_expr(col_ref)})")
  def pow(a, b):     return Column(_pow_sql(_to_col_expr(a), _to_sql_expr(b)))  # b may be a literal
  ```

---

## D. UDF & Embedded Python

### Rule 21: Probe the UDC path before install; never break session startup
- **Origin**: `CHANGELOG.md` (Phase 1 EPython).
- **Context**: Installing EPython UDFs at session init.
- **Mandate**: Auto-probe the UDC path; `IRISPARK_UDC_PATH` env var wins without probing; warn
  and skip installation when no candidate exists (never crash `IrisParkSession.__init__`).
- **Canonical Example (Fa)**: `_probe_udc_path(session)` → client-side `_UDC_PATH` → install.

### Rule 22: IRIS `{ }` body syntax for `LANGUAGE PYTHON`
- **Origin**: `CHANGELOG.md` (Phase 1); `LESSONS_LEARNED.md` (AS $$...$$).
- **Context**: Generating `CREATE FUNCTION ... LANGUAGE PYTHON` DDL.
- **Mandate**: Emit `LANGUAGE PYTHON DETERMINISTIC { ... }` (IRIS), not PostgreSQL `AS $$...$$`.
  Avoid underscore-heavy parameter names (IRIS 2026.2 parser bug, ERROR #5488).
- **Canonical Example (Fa)**:
  ```sql
  CREATE FUNCTION irispark_udc_conv(x) LANGUAGE PYTHON DETERMINISTIC { ... }
  ```

---

## E. Testing & Verification

### Rule 23: Probe the dialect against live IRIS before writing code
- **Origin**: `CHANGELOG.md` (v1.3 Column parity); `rules_functions_aggregations.md`.
- **Context**: Any new function/operator/aggregate.
- **Mandate**: Empirically lock dialect behavior against live IRIS first; never assume a
  function is supported or behaves a certain way.
- **Canonical Example (Fa)**: `groupingSets` verified unsupported on 2026.2 before being
  declared out of scope.

### Rule 24: Pin the IRIS engine version
- **Origin**: `CHANGELOG.md` (engine pin); `LESSONS_LEARNED.md` (latest drift).
- **Context**: Dev/CI IRIS image.
- **Mandate**: Never use `latest`; pin `intersystemsdc/iris-community:2026.2` and re-validate
  on any upgrade.
- **Canonical Example (Fa)**: CI and Makefile pinned to `:2026.2`.

---

## F. Deployment

### Rule 25: Build connect kwargs conditionally
- **Origin**: `CHANGELOG.md` (timeout/sslconfig regression); `LESSONS_LEARNED.md`.
- **Context**: Constructing the IRIS driver connection.
- **Mandate**: Only pass non-None kwargs to the driver; `timeout=None`/`sslconfig=None` must
  not be forwarded.
- **Canonical Example (Fa)**: `IrisParkSession` builds connect kwargs conditionally.
