# Lessons Learned (Flight Recorder)

> Flight recorder of hard-won errors and their causal chains. Each entry follows the
> template: Context → Symptom → Causal Chain (Gu) → Applied Solution → Analogy (Ma) →
> Files Touched → Prevention. Entries are grouped by domain and cite the commit that
> fixed them. Consult this file (and `PATTERNS.md`) before generating novel solutions.

---

## Numerical & UDAF

### 🐛 [2026-08-14] Error: CORR Σ-formula catastrophic cancellation → `inf`/`NaN`

- **Context**: We were trying to compute Pearson correlation entirely inside IRIS via a
  `CREATE AGGREGATE IRISPARK.CORR` UDAF, using the textbook Σ-formula
  `(n·Σxy − Σx·Σy) / sqrt((n·Σx²−Σx²)(n·Σy²−Σy²))`.
- **Symptom**: At 1e9-scale values the result was `inf` (and `NaN` on other scales) while
  pandas gave `0.9291`. Native IRIS `stat.corr` also degraded to NULL at that scale.
- **Causal Chain (Gu)**:
  - Because the Σ-formula accumulates huge raw sums (`Σx²`, `Σxy`) → then the numerator and
    denominator are differences of two huge near-equal quantities → therefore catastrophic
    cancellation destroys the result at large offsets.
- **Applied Solution**: Rewrote to the Welford/Chan online algorithm with state
  `n|mx|my|C|m2x|m2y`, single-pass centered updates, and Chan parallel-merge equations for
  `%PARALLEL`. Denominator computed as `sqrt(m2x) * sqrt(m2y)` (no huge intermediate product).
- **Useful Analogy (Ma)**: "This was similar to the classic `x² − y²` vs `(x−y)(x+y)`
  numerical-stability problem — never arrange state so the answer is a difference of two
  huge near-equal quantities."
- **Files Touched**: `irispark/sql/udaf/corr.py`, `tests/test_udaf.py`, `rules_functions_aggregations.md`.
- **Prevention**: Use online/centered algorithms over Σ-formulas; add a magnitude-stress
  test (`large_offset` 1e9 base) that the old implementation fails.

### 🐛 [2026-08-14] Error: Value-carrying UDAF state exceeds `MAXSTRING` → fatal SQLCODE -400

- **Context**: We were trying to compute median/percentile with a value-carrying UDAF state
  `p|v1|v2|...` carried in a `VARCHAR(4000)`.
- **Symptom**: Past ~250k values in one group the query died FATALLY with SQLCODE -400
  (verified at 1M rows in one group). No graceful CLOB promotion.
- **Causal Chain (Gu)**:
  - Because the state grows O(n) per group and is serialized into a VARCHAR → then past
    ~250k values the string exceeds IRIS `MAXSTRING` (~3.6MB) → therefore the per-row
    concat in ITERATE fails fatally.
- **Applied Solution**: Documented the limit; the SQL-native analytic engine
  (`IRISPARK_*_ANALYTIC` markers) is the production path at any size; the UDAF remains the
  guarded fallback validated up to 100k values/group.
- **Useful Analogy (Ma)**: "Similar to a fixed-size buffer overflow — the state container,
  not the algorithm, is the constraint."
- **Files Touched**: `irispark/sql/udaf/distribution.py`, `rules_functions_aggregations.md` §6.1.
- **Prevention**: Prefer fixed-size state (moments) or the analytic engine; never assume a
  VARCHAR state promotes to CLOB.

### 🐛 [2026-08-14] Error: ObjectScript `While`-condition arithmetic wedges a worker at 100% CPU

- **Context**: We were trying to compile a SQL-embedded ObjectScript aggregate body with a
  loop bound computed inline.
- **Symptom**: The client hung with no error; `docker stats` showed a worker spinning at
  100% CPU forever. Recovery required `docker restart iris`.
- **Causal Chain (Gu)**:
  - Because the `While` condition contained an unparenthesized arithmetic expression
    (`While i <= n + 1`) → then the IRIS compiler wedged the worker → therefore the whole
    server job hung until restart.
- **Applied Solution**: Precompute the bound (`Set m = n + 1`) or parenthesize
  (`While i <= (n + 1)`). All aggregate bodies now follow this rule.
- **Useful Analogy (Ma)**: "Similar to a compiler infinite-loop bug — the fix is to avoid
  the triggering construct entirely."
- **Files Touched**: `irispark/sql/udaf/*.py`, `rules_functions_aggregations.md` §6.1.
- **Prevention**: `While` conditions must contain bare variables/literals and comparisons
  only; hoist any arithmetic into a `Set` before the loop.

### 🐛 [2026-08-14] Error: UDAF reserved-word name crashed session startup

- **Context**: We were trying to register `first`/`last`/`max_by`/`min_by` as
  `IRISPARK.FIRST` etc. at session init.
- **Symptom**: `CREATE AGGREGATE IRISPARK.FIRST` failed with a reserved-word error, which
  crashed `IrisParkSession.__init__` and silently disabled mono/anti aggregations.
- **Causal Chain (Gu)**:
  - Because `FIRST` is a reserved word in IRIS → then the DDL failed at startup → therefore
    the session crashed and the aggregates were never installed.
- **Applied Solution**: Renamed the four aggregates to `IRISPARK.AGG_FIRST/AGG_LAST/
  AGG_MAX_BY/AGG_MIN_BY`; `functions.first/last/max_by/min_by` render the new names.
- **Useful Analogy (Ma)**: "Similar to quoting reserved words in ORDER BY — reserved words
  break DDL at parse time."
- **Files Touched**: `irispark/sql/udaf/extrema.py`, `functions.py`, `tests/test_udaf_extrema.py`.
- **Prevention**: Probe `CREATE` early, not at session startup; avoid reserved words in
  aggregate names.

### 🐛 [2026-08-14] Error: `find_in_set` crashed the IRIS server

- **Context**: We were trying to implement `find_in_set` via `SUBSTRING` on a comma-joined set.
- **Symptom**: A negative `SUBSTRING` length produced an ILLEGAL VALUE fatal query error when
  the element was the first of the set; later elements were off-by-one.
- **Causal Chain (Gu)**:
  - Because `SUBSTRING(arr, 1, pos - 2)` produced a negative length for the first element →
    then IRIS raised a fatal error → therefore the query (and server) crashed.
- **Applied Solution**: Rewrote to count commas in the padded prefix
  `SUBSTRING(',' || arr || ',', 1, pos - 1)`.
- **Useful Analogy (Ma)**: "Similar to off-by-one in string indexing — pad the string to
  normalize the first/last element cases."
- **Files Touched**: `functions.py`, `tests/test_functional_gaps.py::TestFindInSet`.
- **Prevention**: Guard substring lengths; test first/middle/last/not-found/partial-token.

### 🐛 [2026-08-14] Error: `%PARALLEL` hint placement is a parse error

- **Context**: We were trying to verify `%PARALLEL` merge semantics on the UDAFs.
- **Symptom**: `FROM <table> %PARALLEL` was a parse error on 2026.2; the built-in `AVG`
  failed identically (not a UDAF issue).
- **Causal Chain (Gu)**:
  - Because the keyword belongs before the table name → then the post-table position was
    rejected by the parser → therefore the hint silently did nothing / errored.
- **Applied Solution**: Use `FROM %PARALLEL <table>`.
- **Useful Analogy (Ma)**: "Similar to SQL hint placement — position matters to the parser."
- **Files Touched**: `rules_functions_aggregations.md` §6.2, UDAF tests.
- **Prevention**: Always write `FROM %PARALLEL <table>`; verify with a `%PARALLEL` ≡ serial test.

---

## UDF & Embedded Python

### 🐛 [2026-08-14] Error: `format_string` `%`-spec scanner infinite loop

- **Context**: We were trying to implement `format_string`/`printf` as python-side UDFs.
- **Symptom**: A leftover stub in the `%` spec scanner looped forever on any digit-carrying
  spec (e.g. `%05d`).
- **Causal Chain (Gu)**:
  - Because the spec slice did not include the leading `%` and scanning did not stop on
    uppercase conversion chars → then digit-carrying specs never terminated → therefore an
    infinite loop.
- **Applied Solution**: Include the leading `%` in the spec slice; stop scanning on any
  alphabetic conversion char; dispatch `%E`/`%G` correctly; honor precision in `%e`/`%E`.
- **Useful Analogy (Ma)**: "Similar to a regex that never matches — the scanner needs a
  guaranteed terminal state."
- **Files Touched**: `irispark/sql/udf/irispark_udc.py`, `tests/test_irispark_udc.py`.
- **Prevention**: Fuzz the spec parser (20k-spec fuzz, no hangs/crashes) and test
  digit-carrying width specs.

### 🐛 [2026-08-14] Error: `parse_url` returned NULL for every `scheme://` URL

- **Context**: We were trying to implement `parse_url` as a python-side UDF.
- **Symptom**: Every `scheme://` URL returned NULL.
- **Causal Chain (Gu)**:
  - Because the absolute-path regex matched the whole URL instead of the scheme-stripped
    remainder → then the path never matched → therefore NULL for all scheme URLs.
- **Applied Solution**: Match the scheme-stripped remainder; REF/QUERY/PATH no longer include
  the `#`/`?` delimiters in their values.
- **Useful Analogy (Ma)**: "Similar to parsing a URL by parts — strip the scheme before
  matching the path."
- **Files Touched**: `irispark/sql/udf/irispark_udc.py`, `tests/test_irispark_udc.py`.
- **Prevention**: Test userinfo/ports/IPv6/opaque URL shapes.

### 🐛 [2026-08-14] Error: IRIS 2026.2 parser bug with underscore placement in params

- **Context**: We were trying to register EPython UDFs with parameter names like
  `from_base`/`to_base`.
- **Symptom**: `ERROR #5488` from the IRIS 2026.2 parser.
- **Causal Chain (Gu)**:
  - Because the parser mishandles underscore placement in certain parameter names → then
    the DDL failed → therefore the UDF could not be created.
- **Applied Solution**: Renamed parameters to `frombase`/`tobase`.
- **Useful Analogy (Ma)**: "Similar to a reserved-word collision — rename to avoid the
  parser quirk."
- **Files Touched**: `irispark/sql/udf/epython.py`.
- **Prevention**: Avoid underscore-heavy parameter names in `LANGUAGE PYTHON` DDL.

### 🐛 [2026-08-14] Error: `AS $$...$$` body syntax (PostgreSQL) rejected by IRIS

- **Context**: We were trying to generate `CREATE FUNCTION ... LANGUAGE PYTHON` DDL.
- **Symptom**: The emitted `AS $$...$$` body syntax was PostgreSQL-style and failed on IRIS.
- **Causal Chain (Gu)**:
  - Because the generator emitted `AS $$...$$` → then IRIS rejected it → therefore the
    function could not be created.
- **Applied Solution**: Emit IRIS `LANGUAGE PYTHON DETERMINISTIC { ... }` body syntax.
- **Useful Analogy (Ma)**: "Similar to a dialect mismatch — the body syntax must match the
  target engine."
- **Files Touched**: `irispark/udf.py`.
- **Prevention**: Probe `CREATE`/`DROP` a test function instead of hardcoding support.

---

## SQL Generation & Parity

### 🐛 [2026-08-21] Error: `na.fill` filled all columns with a string → datatype mismatch

- **Context**: We were running notebook 06's `df.na.fill("desconhecido").show()` on a
  DataFrame with columns `id` (INTEGER), `nome` (VARCHAR), `valor` (DOUBLE).
- **Symptom**: `SQLCODE -378 Datatype mismatch, explicit CAST is required` — `COALESCE(id,
  'desconhecido')` (VARCHAR into an INTEGER column).
- **Causal Chain (Gu)**:
  - Because `fillna` with `subset=None` filled **every** column with the value → then the
    string `"desconhecido"` was placed into `COALESCE(id, ...)` → therefore IRIS rejected the
    VARCHAR/INTEGER mismatch. PySpark instead fills only type-compatible columns.
- **Applied Solution**: `fillna` now fills only type-compatible columns when `subset` is
  omitted (a string fills string columns, a number fills numeric columns), matching PySpark.
- **Useful Analogy (Ma)**: "Similar to PySpark's type-aware fillna — a string fill value only
  touches string columns."
- **Files Touched**: `irispark/dataframe.py`, `tests/test_dataframe_extras.py::TestFillNa`.
- **Prevention**: When `subset` is omitted, restrict `fillna` to columns whose type is
  compatible with the value; never emit `COALESCE` across mismatched types without a cast.

### 🐛 [2026-08-21] Error: `WindowSpec` relative frame bounds emitted bare integers

- **Context**: We were running notebook 05's sliding-sum cell,
  `Window.partitionBy("dept").orderBy(...).rowsBetween(-1, 0)`.
- **Symptom**: `ROWS BETWEEN -1 AND CURRENT ROW` failed with
  `UNBOUNDED | CURRENT ROW | <non-negative integer> expected, - found`.
- **Causal Chain (Gu)**:
  - Because `_frame_bound(-1)` fell through to `str(-1)` → then the frame rendered as a bare
    `-1` → therefore IRIS rejected the negative integer (frame bounds must be
    `UNBOUNDED PRECEDING/FOLLOWING`, `CURRENT ROW`, or `N PRECEDING`/`N FOLLOWING`).
- **Applied Solution**: `_frame_bound` now converts negative offsets to `N PRECEDING` and
  positive offsets to `N FOLLOWING` (`rowsBetween(-1, 0)` → `1 PRECEDING AND CURRENT ROW`).
- **Useful Analogy (Ma)**: "Similar to a units/format mismatch — the same integer means a
  different thing in IRIS frame syntax."
- **Files Touched**: `irispark/window.py`, `tests/test_window.py`.
- **Prevention**: Relative window-frame offsets must render as `N PRECEDING`/`N FOLLOWING`,
  not bare integers; test negative and positive offsets.

### 🐛 [2026-08-21] Error: `freqItems()` passed `col('*')`, emitting invalid SQL

- **Context**: We were running notebook 03's `df.stat.freqItems()`.
- **Symptom**: `SELECT * AS col__ FROM vendas` failed with `) expected, AS found`.
- **Causal Chain (Gu)**:
  - Because `freqItems` called `self._df.select(col('*'))` → then `col('*')` is a `Column`
    whose `_expr` is `"*"` → therefore the SQL generator auto-aliased it to `* AS col__`
    (invalid IRIS SQL). The bare-string form `select("*")` is correctly recognized as
    all-columns via `_is_all_columns`.
- **Applied Solution**: `freqItems` now calls `select("*")`.
- **Useful Analogy (Ma)**: "Similar to the `_to_col_expr` vs `_to_sql_expr` distinction —
  `*` must be passed as the string form, not wrapped in a Column."
- **Files Touched**: `irispark/dataframe.py`, `tests/test_integration.py::TestActions::test_freq_items`.
- **Prevention**: Don't wrap `*` in `col()`/`Column`; pass the bare string `"*"` for
  all-columns selection.

### 🐛 [2026-08-21] Error: `crosstab()` parameter shadowed the `col()` import

- **Context**: We were running notebook 03's `df.stat.crosstab("estado", "cidade")`.
- **Symptom**: `ARGUMENT ERROR / Incorrect number of parameters` at SQL prepare; the
  generated SQL contained `<function col>` as a column expression.
- **Causal Chain (Gu)**:
  - Because `crosstab(self, row, col)` had a parameter named `col` → then the in-body
    `from irispark.functions import col` was rebound to the *function* (overwriting the
    argument value `"cidade"`) → therefore `col(col)` passed the function object, which
    serialized to `<function col>` in the SQL.
- **Applied Solution**: Aliased the import (`from irispark.functions import col as _col`).
- **Useful Analogy (Ma)**: "Similar to any Python shadowing bug — a parameter that collides
  with an imported symbol silently rebinds it."
- **Files Touched**: `irispark/dataframe.py`, `tests/test_integration.py::TestActions::test_crosstab`.
- **Prevention**: Avoid method parameters that shadow imported function names (`col`, `sum`,
  `min`, ...); alias the import or rename the parameter. (The same shadowing hit
  `sampleBy(self, col, ...)` — its unused `col()` import was removed.)

### 🐛 [2026-08-21] Error: `count()` wrapped an ORDER BY in a subquery (IRIS rejects it)

- **Context**: We were running notebook 03's grouped-aggregation cell, which ends with
  `.orderBy("total DESC").show()`.
- **Symptom**: `show()` → `to_pandas()` → `count()` failed with `) expected, IDENTIFIER
  (ORDER) found` because `count()` wraps `self.to_sql()` (which ends in `ORDER BY`) in
  `SELECT COUNT(*) FROM (...)`, and IRIS rejects `ORDER BY` inside a subquery.
- **Causal Chain (Gu)**:
  - Because `to_pandas()` calls `count()` to check the `warn_threshold` → then the ordered
    query was wrapped in `SELECT COUNT(*) FROM (<ordered query>)` → therefore IRIS rejected
    the embedded `ORDER BY`.
- **Applied Solution**: `count()` strips a trailing top-level `ORDER BY` (leaving window
  `OVER(... ORDER BY ...)` intact), since `ORDER BY` does not change the row count.
- **Useful Analogy (Ma)**: "Similar to the reserved-word quoting class — a subquery is a
  fresh parse context where trailing clauses are invalid."
- **Files Touched**: `irispark/dataframe.py`,
  `tests/test_sql_generator.py::test_count_strips_top_level_order_by`.
- **Prevention**: When wrapping SQL in a subquery (count, tail, analytic, union), strip
  or hoist a trailing top-level `ORDER BY`; never assume `ORDER BY` is valid inside a
  subquery.

### 🐛 [2026-08-21] Error: reserved-word aliases unquoted in the `agg()` column-expression path

- **Context**: We were running notebook 03, calling `.agg(min("valor").alias("min"), ...)`.
- **Symptom**: `MIN(valor) AS min` failed at parse time with `IDENTIFIER expected, reserved
  word MIN found`.
- **Causal Chain (Gu)**:
  - Because the `_agg_exprs` branch (used by `.agg(Column.alias(...))`) appended the aliased
    expression verbatim → then the alias `min` (an IRIS reserved word) was not quoted →
    therefore a fatal parse error. The SELECT-list path was already fixed with
    `_quote_expr_alias`, but the aggregation path had not been covered.
- **Applied Solution**: Applied `_quote_expr_alias()` to the `_agg_exprs` branch too,
  producing `MIN(valor) AS "min"`.
- **Useful Analogy (Ma)**: "Same class as the SELECT-list reserved-alias bug — every
  `... AS <alias>` emission site must quote reserved-word aliases."
- **Files Touched**: `irispark/sql_generator.py`,
  `tests/test_sql_generator.py::test_groupby_col_expr_agg_reserved_alias_quoted`.
- **Prevention**: Audit every `... AS <alias>` emission site for reserved-word quoting; a
  fix in one path does not cover the others.

### 🐛 [2026-08-21] Error: `_quote` mishandles NaN and Column values

- **Context**: We were running notebook 02's conditional cell, building a pandas DataFrame
  with `None` values in a float column and calling `createDataFrame`, plus using
  `when(...).otherwise(col("a"))`.
- **Symptom**: `createDataFrame` failed during batch insert with `Field 'NAN' not found`
  (the INSERT emitted `VALUES (nan, ...)`); separately, `when(...).otherwise(col("a"))`
  rendered `Column('a')` in the SQL, failing with `User defined SQL function 'SQLUSER.COLUMN'
  does not exist`.
- **Causal Chain (Gu)**:
  - Because `_quote()` only special-cased `None`, not NaN → then `str(nan)` → `"nan"` was
    emitted → therefore IRIS rejected `nan` as a field name.
  - Because `_quote()` had no `Column` branch → then `when().otherwise(col("a"))` fell
    through to `str(Column('a'))` → `"Column('a')"` → therefore a bogus `SQLUSER.COLUMN`
    reference.
- **Applied Solution**: `_quote()` now returns `NULL` for NaN floats and unwraps `Column`
  args via `_expr`.
- **Useful Analogy (Ma)**: "Similar to the `None`→`NULL` branch — NaN is the float
  representation of a missing value and must also map to SQL NULL, not a bare identifier."
- **Files Touched**: `irispark/column.py`, `tests/test_sql_generator.py`
  (`test_quote_handles_nan_and_column`).
- **Prevention**: `_quote()` must handle every value type it can receive — NaN → NULL and
  `Column` → its `_expr` — before the generic `str()` fallthrough.

### 🐛 [2026-08-21] Error: reserved-word / builtin-name column aliases unquoted

- **Context**: We were running notebook 02's string cell, aliasing each function with the
  function's own name (`.alias("upper")`, `.alias("lower")`, ...).
- **Symptom**: `SELECT UPPER(nome) AS upper, ...` failed at parse time with `IDENTIFIER
  expected, reserved word UPPER found` (and similarly for `LOWER`/`TRIM`/`CONCAT`/
  `SUBSTRING`/`LPAD`/`LENGTH`/`LEVENSHTEIN`/`SOUNDEX`/`SPLIT`/`CHARINDEX`/`DOMAIN`).
- **Causal Chain (Gu)**:
  - Because `Column.alias(name)` renders `expr AS name` and the SELECT-list path emitted it
    verbatim unquoted → then aliases that collide with IRIS SQL built-in function names or
    keywords were parsed as functions/reserved words → therefore a fatal parse error.
  - Because the static `_IRIS_RESERVED_WORDS` set only listed SQL keywords (COUNT, FROM,
    ...), not built-in function names → then `UPPER`/`LOWER`/`DOMAIN` were not quoted →
    therefore the failure.
- **Applied Solution**: Expanded `_IRIS_RESERVED_WORDS` to include SQL built-in function
  names and additional keywords (`DOMAIN`, `KEYWORD`, `SCHEMA`, ...); added
  `_quote_expr_alias()` which double-quotes the alias identifier at the SELECT-list and
  withColumn emission sites.
- **Useful Analogy (Ma)**: "Similar to the existing `COUNT(*) AS "count"` quoting — any
  alias that collides with a reserved word/function name must be double-quoted."
- **Files Touched**: `irispark/sql_generator.py`, `tests/test_sql_generator.py`
  (`test_reserved_word_select_alias_is_quoted`).
- **Prevention**: Quote any alias that is an IRIS keyword OR built-in function name; when a
  new reserved word surfaces at runtime, add it to `_IRIS_RESERVED_WORDS`.

### 🐛 [2026-08-14] Error: `count_if`/`bool_*` raw-SQL string predicates quoted as literals

- **Context**: We were trying to pass raw-SQL string predicates to `count_if`/`bool_and`/
  `bool_or`.
- **Symptom**: Strings were passed through `_to_sql_expr` and quoted as literals
  (e.g. `'id > 2'` → SQLCODE -14 syntax error).
- **Causal Chain (Gu)**:
  - Because string predicates were treated as literals → then the SQL was invalid → therefore
    a syntax error.
- **Applied Solution**: New `_predicate_expr()` embeds strings verbatim and prefers the
  `_predicate` form of `PredicateColumn`.
- **Useful Analogy (Ma)**: "Similar to `CaseWhen` — predicates must be embedded verbatim,
  not quoted."
- **Files Touched**: `functions.py`, `tests/test_functional_gaps.py::TestPredicateAggregates`.
- **Prevention**: Test both string and Column predicates.

### 🐛 [2026-08-21] Error: scalar functions rendered column args as string literals

- **Context**: We were trying to build scalar SQL functions (math/string/date/hash/trig/
  conditional/aggregate/window) and ran notebook 02 against live IRIS.
- **Symptom**: `log("x")` → fatal `SQLCODE -400 ILLEGAL VALUE`; `sqrt("x")` → `0` (wrong);
  `last_day`/`dayname`/`monthname` → fatal; `upper("nome")`/`md5("email")` returned the
  transform/hash of the literal string `'nome'`/`'email'`, not the column values.
- **Causal Chain (Gu)**:
  - Because scalar functions rendered the column argument via `_to_sql_expr()` → then bare
    strings were single-quoted into SQL literals (`log("x")` → `LOG('x')`) → therefore either
    a fatal error (log of the string `'x'`) or silently wrong results (`SQRT('x')` coerced to
    0, `UPPER('nome')` = literal).
- **Applied Solution**: All scalar functions now render the **column** argument via
  `_to_col_expr()` (bare identifier) and keep **literal** arguments (scale, pattern,
  replacement, offset, base) via `_to_sql_expr()`. Fixed ~40 functions in `functions.py`.
- **Useful Analogy (Ma)**: "This was similar to the `count_if` raw-predicate quoting bug — a
  bare string being mis-classified as a literal instead of a column reference."
- **Files Touched**: `irispark/functions.py`, `docs/PATTERNS.md` (Rule 20a),
  `tests/test_functional_gaps.py` (TestSplit).
- **Prevention**: In any scalar function, use `_to_col_expr()` for the column argument and
  `_to_sql_expr()` for literal arguments (Rule 20a in `PATTERNS.md`); probe every function
  with a bare-string column arg against live IRIS, not just `Column` objects.

### 🐛 [2026-08-21] Error: `irispark_split` raised `ILLEGAL VALUE` on a leading match

- **Context**: We were trying to split a string with a regex that matched at the start.
- **Symptom**: A fatal `SQLCODE -400` (`ILLEGAL VALUE`) when the match began at position 1.
- **Causal Chain (Gu)**:
  - Because `$EXTRACT(str, pos, m.Start - 1)` became `$EXTRACT(str, 1, 0)` (end before
    start) when `m.Start == 1` → then IRIS raised `ILLEGAL VALUE` → therefore a fatal query
    error.
- **Applied Solution**: Guarded the prefix slice: `If end >= pos` before `$EXTRACT`.
- **Useful Analogy (Ma)**: "Similar to the `find_in_set` negative-`SUBSTRING`-length crash —
  a string-slice boundary bug where the end precedes the start."
- **Files Touched**: `irispark/sql/udf/split.py`, `tests/test_functional_gaps.py::TestSplit`.
- **Prevention**: Test patterns that match at the start/end and empty trailing tokens.

### 🐛 [2026-08-14] Error: `pow`/`power` domain errors → fatal SQLCODE -400

- **Context**: We were trying to match Spark's `pow`/`power` behavior.
- **Symptom**: `POWER(0,-1)` and `POWER(neg, fractional)` raised a fatal SQLCODE -400 (query
  dies) where Spark yields ±Inf/NaN.
- **Causal Chain (Gu)**:
  - Because IRIS cannot represent Inf/NaN → then domain errors raise a fatal error → therefore
  the whole query died.
- **Applied Solution**: Guarded both to NULL (documented deviation).
- **Useful Analogy (Ma)**: "Similar to division-by-zero handling — guard the domain before
  the engine raises."
- **Files Touched**: `functions.py`, `scripts/parity.py`.
- **Prevention**: Guard domain-error inputs to NULL; document the Inf/NaN deviation.

### 🐛 [2026-08-14] Error: `pmod` negative divisor mismatch

- **Context**: We were trying to match Spark's `pmod` with a negative divisor.
- **Symptom**: `MOD(MOD(a,b)+b,b)` gave `MOD(-2,-3)=-2` vs Spark `1`.
- **Causal Chain (Gu)**:
  - Because the naive rendering assumed a positive divisor → then the sign was wrong → therefore
    a parity mismatch.
- **Applied Solution**: `CASE WHEN b > 0 THEN MOD(MOD(a,b)+b,b) ELSE MOD(a,b) END`.
- **Useful Analogy (Ma)**: "Similar to modulo sign conventions — branch on the divisor sign."
- **Files Touched**: `functions.py`, `scripts/parity.py`.
- **Prevention**: Test negative divisors in the parity harness.

### 🐛 [2026-08-14] Error: `round` at exact `.5` double boundary

- **Context**: We were trying to match Spark's `round(x, 2)`.
- **Symptom**: IRIS `ROUND` rounds the exact binary double (`2.67499… → 2.67`) while Spark
  rounds the decimal string (`2.675 → 2.68`); difference of 0.01 on `2.675`.
- **Causal Chain (Gu)**:
  - Because IRIS rounds the binary double and Spark rounds the decimal string → then the
    results differ at exact `.5` boundaries → therefore a documented deviation.
- **Applied Solution**: Accepted and pinned as a known deviation.
- **Useful Analogy (Ma)**: "Similar to float-vs-decimal rounding — the representation
  determines the result."
- **Files Touched**: `scripts/parity.py` (KNOWN_DEVIATIONS).
- **Prevention**: Pin known deviations explicitly; use ULP-tolerant float comparison.

### 🐛 [2026-08-14] Error: `lpad`/`rpad` empty pad → NUL characters

- **Context**: We were trying to match Spark's `lpad`/`rpad` with an empty pad string.
- **Symptom**: IRIS pads with NUL (U+0000) characters where Spark returns the string unchanged.
- **Causal Chain (Gu)**:
  - Because IRIS pads with NUL for an empty pad → then the result contained NUL bytes →
    therefore a parity mismatch.
- **Applied Solution**: Guarded with `CASE WHEN pad = ''`.
- **Useful Analogy (Ma)**: "Similar to a degenerate-input guard — handle the empty pad
  explicitly."
- **Files Touched**: `functions.py`, `scripts/parity.py`.
- **Prevention**: Test empty-pad inputs.

### 🐛 [2026-08-14] Error: `NaN` literal crashes IRIS temp tables

- **Context**: We were trying to round-trip NaN inputs through the parity harness.
- **Symptom**: IRIS temp tables crashed on the `NAN` literal.
- **Causal Chain (Gu)**:
  - Because IRIS cannot represent NaN as a literal → then temp-table creation crashed →
    therefore NaN inputs are not round-trippable.
- **Applied Solution**: Dropped NaN-input rows from the parity expectations; documented.
- **Useful Analogy (Ma)**: "Similar to a non-representable value — exclude it from the
  round-trip."
- **Files Touched**: `scripts/parity.py`.
- **Prevention**: Document non-round-trippable values; exclude from expectations.

### 🐛 [2026-08-14] Error: `concat_ws` `IS NULL` on expression → SQLCODE -25

- **Context**: We were trying to probe `concat_ws` with a NULL argument.
- **Symptom**: IRIS rejects `IS NULL` on expressions in the SELECT list (SQLCODE -25).
- **Causal Chain (Gu)**:
  - Because IRIS disallows `IS NULL` on arbitrary expressions in the SELECT list → then the
    probe failed → therefore a syntax error.
- **Applied Solution**: Wrapped the probe in `CASE WHEN … IS NULL`.
- **Useful Analogy (Ma)**: "Similar to a context-sensitive operator — `IS NULL` is only valid
  on columns in some positions."
- **Files Touched**: `scripts/parity.py`.
- **Prevention**: Wrap expression-level NULL checks in `CASE WHEN`.

### 🐛 [2026-08-14] Error: `months_between`/`timestampdiff` Spark-exact semantics

- **Context**: We were trying to match Spark's `months_between` and `timestampdiff`.
- **Symptom**: Naive month-length math diverged from Spark.
- **Causal Chain (Gu)**:
  - Because Spark uses a constant 31-day denominator (not month lengths) → then month-length
    math diverged → therefore a parity mismatch.
- **Applied Solution**: `months_between` uses a constant 31-day denominator with a
  both-last-of-month exact case; `timestampdiff` does NOT apply the both-last special
  (verified on month-end/leap vectors).
- **Useful Analogy (Ma)**: "Similar to decoding an opaque spec — derive the exact formula
  from live PySpark, not from intuition."
- **Files Touched**: `irispark/sql/udf/datetime_ext.py`, `functions.py`, `scripts/parity.py`.
- **Prevention**: Derive semantics empirically from live PySpark; pin month-end/leap vectors.

### 🐛 [2026-08-14] Error: Non-ASCII string parity scoped to ASCII for V1

- **Context**: We were trying to verify non-ASCII string round-trips.
- **Symptom**: `'héllo'` round-trips as 6 chars; `$EXTRACT(2,2)` returns `Ã` (byte-transparent
  pipeline).
- **Causal Chain (Gu)**:
  - Because the driver/IRIS data path stores UTF-8 byte-per-char → then non-ASCII chars are
    split across bytes → therefore exact parity only holds for ASCII.
- **Applied Solution**: Scoped parity to ASCII for V1; documented the limitation for a future
  driver charset-negotiation fix.
- **Useful Analogy (Ma)**: "Similar to a byte-vs-char encoding mismatch — the pipeline is
  byte-transparent."
- **Files Touched**: `scripts/parity.py`, `CHANGELOG.md`.
- **Prevention**: Document charset limitations; exclude non-ASCII rows from expectations.

### 🐛 [2026-08-14] Error: Correlated scalar subquery rejected (166s at 1M)

- **Context**: We were trying to build a SQL-native median/percentile engine.
- **Symptom**: A correlated scalar subquery took 166s for 1M rows (IRIS evaluates the
  correlation per group with no reuse).
- **Causal Chain (Gu)**:
  - Because IRIS evaluates the correlated subquery per group → then 1M rows × groups was
    O(n·g) → therefore 166s.
- **Applied Solution**: Adopted a single-pass analytic query
  (`ROW_NUMBER() OVER (PARTITION BY ...)` + windowed `COUNT(*)`), 4.3s at 1M×1000 groups.
- **Useful Analogy (Ma)**: "Similar to a nested-loop vs hash-join choice — one sort per query
  beats a correlated subquery."
- **Files Touched**: `sql_generator.py`, `functions.py`, `rules_functions_aggregations.md`.
- **Prevention**: Benchmark candidate SQL shapes before adopting; reject correlated subqueries
  for grouped analytics.

### 🐛 [2026-08-24] Error class: reserved-word quoting was patched per emission site (whack-a-mole)

- **Context**: A project review found 5 of the previous 10 fix commits were the same bug
  class — an unquoted reserved-word alias at a new `... AS <alias>` emission site each time
  (`SELECT` list, `agg()` path, `EXTRACT`, ORDER BY, ...).
- **Symptom**: Each notebook run could discover another reserved word and crash with
  `IDENTIFIER expected, reserved word X found`.
- **Causal Chain (Gu)**:
  - Because quoting fixes were applied only to the specific path that crashed → then every
    other emission point remained bare → therefore the same parse error resurfaced per path.
  - Because `_IRIS_RESERVED_WORDS` was hand-grown → then officially-reserved words like
    `FIRST`, `LAST`, `TOP`, `WHEN`, `WHERE`, `AND` were missing → therefore aliases using
    them still failed even on covered paths.
- **Applied Solution**: Routed all identifier emission points through
  `sql_generator._quote_if_reserved` (withColumn sequential/merged, rename targets in five
  paths, unpivot label/value, agg composite aliases, pivot value aliases) and synced the
  blocklist against the official IRIS 2026.2 reserved-word list. Added a regression net
  (`tests/test_reserved_word_scan.py`) that exercises every path with reserved names.
  Deliberately rejected blanket default-quoting: quoted identifiers are case-sensitive in
  IRIS, unquoted ones are not — default-quoting would break matching against pre-existing
  tables. The net caught the missing `FIRST` entry on its first run, proving its value.
- **Useful Analogy (Ma)**: "Same as fixing one leaky joint at a time vs replacing the pipe —
  a single choke-point function plus a pressure test beats per-joint patches."
- **Files Touched**: `irispark/sql_generator.py`,
  `tests/test_reserved_word_scan.py`, `tests/test_unpivot.py` (expectation updated to the
  corrected quoted form), `docs/PATTERNS.md` Rule 15.
- **Prevention**: New alias-emission code must call `_quote_if_reserved`; keep
  `test_reserved_word_scan.py` green. Do not quote non-reserved identifiers.

### 🐛 [2026-08-24] Error: `_batch_insert` fallback duplicated rows on partial failure

- **Context**: Reviewing `createDataFrame` ingestion performance (P0 in `concerns.md`).
- **Symptom**: If a cursor INSERT failed mid-stream, the code silently re-sent *every* row
  through `session.sql()` — duplicating all rows already committed and retrying the failed
  one through a second path. Also, 1 row = 1 round-trip made 100k-row ingests crawl.
- **Causal Chain (Gu)**:
  - Because the bare `except Exception:` caught mid-loop failures → then the fallback loop
    re-ran from row 0 → therefore already-inserted rows were inserted twice.
  - Because IRIS SQL has no multi-row `VALUES (...)` clause (confirmed via the official
    INSERT reference and InterSystems staff guidance) → then naive chunked VALUES would
    have failed at parse → therefore the supported `INSERT ... SELECT ... UNION ALL
    SELECT ...` form is used per 200-row chunk instead.
- **Applied Solution**: Chunked multi-row INSERTs (UNION ALL form; singleton keeps VALUES),
  and failure now raises a `RuntimeError` naming the failed row range with `__cause__`
  chained — no silent retry.
- **Useful Analogy (Ma)**: "Same as an idempotency-key rule in payment APIs — never blind-
  retry a batch whose earlier portion may have landed."
- **Files Touched**: `irispark/session.py`, `tests/test_batch_insert.py`.
- **Prevention**: Bulk-write paths must either be idempotent or fail loudly; test both the
  happy path and the mid-stream failure contract.

### 🐛 [2026-08-24] Error: `tail()` rows contained column names instead of data

- **Context**: Full online verification pass against live IRIS (5 failures appeared; all
  reproduced on a pre-change baseline worktree, so none were regressions of the day's
  commits).
- **Symptom**: `vendas_df.order_by("valor").tail(2)` returned 2 "rows" whose only element
  was the list of field names; indexing `r[3]` raised `tuple index out of range`.
- **Causal Chain (Gu)**:
  - Because `tail()` constructed `Row(*row, _fields=col_names)` → and `Row.__new__`
    treats *any* kwarg as name=value data → then `_fields` triggered the kwargs branch,
    the positional values were discarded, and `_fields=(' _fields',)` with the field-name
    list as the sole value.
  - Because every other row path used `_make_row(row, columns)` (constructs bare, assigns
    fields after) → then only `tail()` crashed → therefore the bug hid until an ordered
    tail ran online.
- **Applied Solution**: Two-sided — `Row.__new__` pops a `_fields` override kwarg before
  its kwargs-as-data logic, and `tail()` now uses `_make_row` like `collect()`. Pinned by
  offline `tests/test_row.py`.
- **Useful Analogy (Ma)**: "Same as a function whose optional flag parameter is read as
  data because it shares the kwargs bucket — reserved names need explicit handling."
- **Files Touched**: `irispark/row.py`, `irispark/dataframe.py`, `tests/test_row.py`,
  plus pandas-portability fixes in `tests/test_udaf_extrema.py` /
  `tests/test_dataframe_extras.py` (pandas ≥2.2 consumes `include_groups`; older pandas
  forwards unknown kwargs to the callable).
- **Prevention**: Construct rows via `_make_row` everywhere; never mix positional data
  with keyword arguments on a class whose `__new__` interprets kwargs as data. Also:
  verify on supported interpreters only — this session's first runs happened to land on
  Python 3.9/pandas 2.1.3 (system python), which both masked and mimicked different bugs.

### 🐛 [2026-08-24] Error: `na.replace` aliased `fillna` — wrong semantics and a crash

- **Context**: Executing the 11-notebook series inside the `jupyter_server` container as
  end-to-end verification; notebook 06 (`df.na.replace(200.0, 999.0).show()`) crashed.
- **Symptom**: `'float' object is not iterable` — the replacement target was forwarded
  into `fillna(value, subset)` as `subset`.
- **Causal Chain (Gu)**:
  - Because `NaFunctions.replace(value, subset)` had the wrong arity for PySpark's
    `replace(to_replace, value)` → then `999.0` (the replacement value) landed in the
    `subset` slot → therefore fillna iterated a float.
  - Because the implementation delegated to `fillna` → then even correct arity would have
    produced null-filling, not value replacement → therefore the API could never satisfy
    its documented purpose. No test covered it; only notebook 06 exercised it.
- **Applied Solution**: Implemented true replacement: argument normalization (scalar,
  mapping, parallel lists), type-compatibility scoping reusing `_fillna_type_compatible`,
  per-column `CASE WHEN col = old THEN new ELSE col END` in the generator (aliased via
  `_quote_if_reserved`, composed with fillna's COALESCE). Pinned by offline
  `tests/test_na_replace.py`.
- **Useful Analogy (Ma)**: "Same as implementing UPDATE by calling INSERT — adjacent
  null-handling APIs are not interchangeable; delegation must match semantics."
- **Files Touched**: `irispark/dataframe.py`, `irispark/sql_generator.py`,
  `tests/test_na_replace.py`. Verified 11/11 notebooks green in-container afterwards.
- **Prevention**: Notebook series is the parity contract — run all of them after API work;
  untested public APIs drift silently until a demo cell exposes them.

### 🐛 [2026-08-24] Error class: post-`union` transformations silently ignored (plus five siblings)

- **Context**: Executing the new DS/DE notebook series (11–17) in the `jupyter_server`
  container as acceptance; first run failed 5 of 7 notebooks.
- **Symptom**: Wildly different per notebook — SQLCODE -29 `Field 'MES' not found`,
  `-12 A term expected`, `-202 parentheses missing`, `-422 SELECT request processed`,
  `KeyError: 0`, and `<CaseWhen object at 0x...>` embedded in SQL.
- **Causal Chain (Gu)**:
  - Because `generate()` short-circuited on `_union_parts` → then any stage chained after
    `.union()` (dedup, withColumns, renames) never reached generation → therefore
    pipelines written in natural PySpark order produced SQL from the union arms alone.
  - Sibling gaps, all latent until a notebook exercised them: bare `when()` without
    `.otherwise()` had no serialization path; pivot's `{"*": "count"}` rendered `THEN *`;
    digit-leading pivot aliases emitted unquoted; dict rows reached positional type
    inference; large UNION ALL statements hit an IRIS preparer limit (-202) at
    content-dependent boundaries (~9.5KB for one shape while other shapes passed at 9.6KB).
  - Diagnosis was slowed by environment drift: a partial file sync into the verification
    container made fixed failures reappear, mimicking new bugs. Full-sync discipline
    (`session.py + functions.py + sql_generator.py + dataframe.py` together) ended it.
- **Applied Solution**: Union becomes a base subquery whenever post-union stages exist;
  dedup became a composable layer in `_simple_table_source` (base-schema physical columns,
  `_rn = 1` hidden); CaseWhen serializes lazily with implicit ELSE NULL; pivot uses
  literal-1 counting and quotes digit-leading aliases; dict rows normalize before
  inference; insert chunks capped by bytes and row count. All pinned offline in
  `tests/test_notebook_regressions.py`; validated by executing all 18 notebooks green.
- **Useful Analogy (Ma)**: "Same as a compiler that stops at the first AST node — early
  returns in a generator must consider every feature flag, or later stages vanish."
- **Files Touched**: `irispark/sql_generator.py`, `irispark/functions.py`,
  `irispark/session.py`, `tests/test_notebook_regressions.py`,
  `scripts/gen_notebooks.py` (nb16 quarantine normalized to schema-safe arms).
- **Prevention**: Execute the full notebook series before merging generator changes;
  when a fix lands, sync ALL changed files into any verification container; add an
  offline pin for each live failure the moment it is understood.

### 🐛 [2026-08-24] Error: withColumns around aggregations generated stage-invalid SQL

- **Context**: Notebook 18 acceptance run — moving averages and month-over-month deltas
  computed on top of `groupBy().agg()` rollups.
- **Symptom**: Evolving SQLCODE -29 `Field not found` (RECEITA / MES / VALOR across
  attempts), then -1 `Invalid statement` once ORDER BY entered the composition.
- **Causal Chain (Gu)**:
  - Because grouped SELECTs and post-aggregation withColumns shared one SQL level →
    window/derived expressions referenced aggregate aliases before they were defined →
    therefore -29.
  - Because pre-aggregation columns chained after a *later* aggregation were hoisted above
    it → their raw source fields (e.g. VALOR) vanished from scope → therefore -29 again.
  - Because the legacy `dropDuplicates` shortcut bypassed composition whenever an
    aggregated base existed → GROUP BY was silently dropped.
  - Because IRIS rejects ORDER BY inside subqueries → naive TOP-wrapping of the layered
    statement failed with -1.
- **Applied Solution**: Consumption model — `agg()` consumes the current pipeline into
  `_grouped_base_columns` (materialized inside the grouped base layer); post-aggregation
  withColumns emit in dependency layers above it; ORDER BY/TOP stay at the outermost
  level; the dedup shortcut yields when a consumed base exists. Pinned by three tests in
  `tests/test_notebook_regressions.py::TestGroupedThenWithColumns`.
- **Useful Analogy (Ma)**: "CTE scoping — each WITH clause sees only what precedes it;
  stage boundaries define what expressions may reference."
- **Files Touched**: `irispark/sql_generator.py`, `irispark/dataframe.py`,
  `tests/test_notebook_regressions.py`.
- **Prevention**: Multi-stage compositions are the highest-risk generator paths; pin every
  live failure offline the moment its cause is understood.

### 🐛 [2026-08-24] Process error: a stray `git checkout -- irispark/` silently reverted committed work

- **Context**: Building the ML core framework (Phase 3). After committing the framework
  modules, a notebook-verification command used a wrong `$CLONE` path; its cleanup step
  ran `git -C <this-repo> checkout -- irispark/`, reverting `feature.py` and
  `ml/__init__.py` to the pre-framework flat style.
- **Symptom**: The commit `866e7ad` shipped only the 5 new framework modules; the
  transformer migration and framework exports were silently gone. The offline suite still
  passed (old `feature.py` is self-consistent), so nothing failed loudly.
- **Causal Chain (Gu)**:
  - Because the verification command reused a `$CLONE` variable that had been pointed at
    the *working repo* instead of the container clone → then its `git checkout -- irispark/`
    cleanup reverted the working tree → therefore committed-but-uncommitted edits vanished.
  - Because the reverted files were self-consistent → then the test suite stayed green →
    therefore the loss was invisible until a manual `git status`/file inspection.
- **Applied Solution**: Re-applied the migration; added `TestTransformerHierarchy` pins
  asserting every transformer sits on the core base classes and that `irispark.ml` exports
  the framework — a future silent revert now fails loudly.
- **Useful Analogy (Ma)**: "Same as a destructive `rm` in a script — a cleanup step that
  can touch the wrong target must be guarded, and the invariant it protects needs a test."
- **Files Touched**: `irispark/ml/feature.py`, `irispark/ml/__init__.py`,
  `tests/test_ml_framework.py`, `tests/test_ml_feature.py`, `scripts/gen_notebooks.py`,
  `notebooks/08_ml_transformers.ipynb`.
- **Prevention**: Never run `git checkout -- <dir>` against a path derived from a mutable
  variable in a verification script; verify `git status` after any container-sync cleanup;
  pin structural invariants (class hierarchy, exports) with tests so silent reverts surface.

### 🐛 [2026-08-24] Error: `OneHotEncoder` silently produced all-zeros on a string column

- **Context**: Running notebook 08, the `cidade_ohe` column came back `0,0` for every row
  regardless of city.
- **Symptom**: No error — just wrong data. `OneHotEncoder(inputCol="cidade")` on the raw
  string column `"SP"/"RJ"/"MG"` yielded all-zeros.
- **Causal Chain (Gu)**:
  - Because `OneHotEncoderModel._transform` emits `CASE WHEN col = 0 THEN '1' ELSE '0' END`
    → and the input column held strings, not integer indices → then every comparison
    `"SP" = 0` was false → therefore all-zeros, silently.
  - Because the existing tests fed *integer* index columns → then they passed → therefore
    the misuse was invisible until a notebook fed a real string column.
- **Applied Solution**: Added a numeric-index guard (`_require_numeric_index`) that raises a
  clear `ValueError` at both `fit()` and `transform()` (schema-drift protection), while
  allowing empty DataFrames and the correct `StringIndexer → OneHotEncoder` contract
  unchanged. Fixed notebooks 08 and 12 to chain `StringIndexer` first and to use
  `VectorAssembler` as a pure `Transformer` (no `fit`). Pinned by 3 new tests.
- **Useful Analogy (Ma)**: "Same as a silent type-coercion bug — comparing a string to an
  int literal never errors, it just never matches; the fix is to validate the input type
  up front."
- **Files Touched**: `irispark/ml/feature.py`, `tests/test_ml_feature.py`,
  `scripts/gen_notebooks.py`, `notebooks/08_ml_transformers.ipynb`,
  `notebooks/12_ds_feature_engineering.ipynb`.
- **Prevention**: Transformers that assume a numeric index must validate the input column
  type at `fit()` (and `transform()` for drift); notebooks must chain `StringIndexer`
  before `OneHotEncoder`; add a test that feeds a string column and asserts a loud error.

### 🐛 [2026-08-24] Error: `MEDIAN()` and bare `CREATE TABLE (cols)` fail in raw SQL

- **Context**: Building the Phase 4 transformers; `Imputer(strategy="median")` and
  `SQLTransformer` both failed against live IRIS.
- **Symptom**: `Imputer` median → SQLCODE -359 `SQL Function not found`; `SQLTransformer`
  → SQLCODE -1 `Invalid SQL statement` at `CREATE TABLE "t" ("age", "income")`.
- **Causal Chain (Gu)**:
  - Because `median()` returns the analytic marker `IRISPARK_MEDIAN_ANALYTIC(v)` which only
    the SQL *generator* expands → then a raw `session.sql("SELECT MEDIAN(...)")` sent the
    unexpanded marker → therefore -359. The UDAF form `IRISPARK.MEDIAN(...)` is what works
    in hand-written SQL.
  - Because `_rows_to_df` created the temp table with bare column names and no types →
    then IRIS rejected `CREATE TABLE "t" ("age", "income")` → therefore -1. IRIS requires
    explicit column types.
- **Applied Solution**: Imputer median uses `IRISPARK.MEDIAN(...)`; `_rows_to_df` emits
  `VARCHAR(4000)` column definitions. Pinned by `TestImputer::test_median` and
  `TestSQLTransformer::test_ratio`.
- **Useful Analogy (Ma)**: "Same as calling a macro-expanded function outside the compiler —
  the analytic marker is a generator-only token; hand-written SQL must use the concrete UDAF."
- **Files Touched**: `irispark/ml/feature.py`, `tests/test_ml_feature.py`.
- **Prevention**: When a transformer issues raw `session.sql`, use concrete UDAF names
  (`IRISPARK.MEDIAN`), never generator-only markers; always give `CREATE TABLE` explicit
  column types.

### 🐛 [2026-08-25] Error: IntegratedML AutoML "hang" was actually `NoEstimatorChosen` on tiny data

- **Context**: Building Phase 5 supervised ML; `AutoMLClassifier.fit()` appeared to hang for
  minutes on a 10-row / 2-feature dataset.
- **Symptom**: `TRAIN MODEL` always raised a client `COMMUNICATION LINK ERROR` (ETIMEDOUT,
  ~2s), and no model ever appeared — the poll timed out.
- **Causal Chain (Gu)**:
  - Because the `iris` driver's `timeout` kwarg is **connect-only** (not a per-query read
    timeout) → then `TRAIN MODEL` (a long server call) always exceeded the socket read
    timeout → therefore the client raised ETIMEDOUT on every run, **even fast ones**. This
    is a constant, not the bug.
  - Because the IntegratedML provider requires enough training rows to *select* an
    estimator → then on 10 rows it returned `NoEstimatorChosen` in **0.03s** (verified by
    calling `iris_automl.automl.train()` directly) → therefore **no model was ever
    created** → the poll correctly never found one. The "hang" was an invisible error: the
    server-side trace went to discarded stdout (no persisted log), so nothing surfaced.
  - Because `TRAIN MODEL` desyncs the connection that issued it → then reusing that
    session for `PREDICT`/cleanup raised EPIPE → therefore training must run on a
    dedicated connection and the model polled on fresh connections.
- **Applied Solution**: AutoML wrapper runs CREATE/TRAIN on a dedicated connection
  (tolerating the client timeout), polls `%ML.TrainedModel` on fresh connections, and
  keeps the caller's session healthy. The training data must be **large enough** for the
  provider to select an estimator — on 200 rows / 2 features, training completes in
  ~2.8s reliably. Also fixed a pyarrow serialization bug (`Decimal` → `VARCHAR` columns)
  by coercing values to `str` in the result materializer. Notebook 22's AutoML cell now
  uses a 200-row dataset and is un-guarded. Pinned by
  `TestAutoML::test_classifier_round_trip` (200 rows).
- **Useful Analogy (Ma)**: "Same as a fire-and-forget async job — the client must not
  block on the long call, and must poll a separate channel for completion; and an async
  job that errors invisibly looks exactly like one that is still running."
- **Files Touched**: `irispark/ml/automl.py`, `irispark/ml/feature.py` (SQLTransformer
  result materializer), `tests/test_ml_supervised.py`, `scripts/gen_notebooks.py`,
  `notebooks/22_ds_supervised_ml.ipynb`.
- **Prevention**: For long server-side operations, never reuse the issuing connection —
  run on a dedicated connection and poll on fresh ones. Make the server-side trace
  visible (persist `AUTOML_HOME`/logs) before debugging an apparent hang — an invisible
  error is indistinguishable from a long-running job. Use training data large enough for
  the provider's estimator-selection heuristic.

### 🐛 [2026-08-25] Error: `randomSplit`'s `%ID` filter breaks under withColumn/prediction wrappers

- **Context**: Building Phase 9 tuning; `CrossValidator`/`TrainValidationSplit` failed with
  `Field '%ID' not found` when evaluating predictions on a split fold.
- **Symptom**: SQLCODE -29 at `SELECT AVG(CASE WHEN prediction=label ...) FROM (SELECT * FROM (SELECT *, ... AS prediction FROM (SELECT *, ... AS probability FROM irispark_tmp_...) AS _wc0) AS _wc1 WHERE MOD((%ID * ...), ...) ...)`.
- **Causal Chain (Gu)**:
  - Because `randomSplit` implements splitting with a `MOD((%ID * seed) ..., ...)` filter →
    then `%ID` is the hidden row-id of a physical table → therefore the filter works on a
    plain table.
  - Because `est.fit(train).transform(val)` wraps the base table in nested withColumn
    subqueries (`_wc0`, `_wc1`) → then `%ID` is not projected through those wrappers →
    therefore the `MOD(%ID ...)` filter, hoisted to the outer level, references an
    invisible field → SQLCODE -29.
- **Applied Solution**: Materialize each split fold to a physical temp table
  (`_materialize`) before fitting/evaluating, giving it a real `%ID`. The same root cause
  motivated the earlier dedup-over-union `%ID` handling — `%ID` is only valid on a real
  table, never through subquery projections.
- **Useful Analogy (Ma)**: "Same as the union/dedup `%ID` issue — a hidden system column
  does not survive any projection or union; only a physical table exposes it."
- **Files Touched**: `irispark/ml/tuning.py`, `tests/test_ml_tuning.py`.
- **Prevention**: Any operation relying on `%ID` (sampling, dedup ordering, fold splits)
  must run on a materialized table; never apply it through withColumn/union/prediction
  subqueries. When adding a subquery-producing stage, test its interaction with the
  `%ID`-based transforms.

### 🐛 [2026-08-25] Design note: planner hook must live on both `Transformer` and `Estimator`

- **Context**: Wiring the planner into the ML framework; `Estimator.fit` tried to call
  `self._resolve_backend()` which was only defined on `Transformer`.
- **Symptom**: `AttributeError: 'LogisticRegression' object has no attribute
  '_resolve_backend'` — `Estimator` does not inherit from `Transformer` in this framework,
  so a `Transformer`-only method is invisible to estimators.
- **Causal Chain (Gu)**: Because `Transformer` and `Estimator` are sibling base classes
  (only `Model` inherits `Transformer`) → then a shared helper defined on `Transformer`
  is not available to `Estimator` → therefore `Estimator.fit` failed. `Model` (which
  extends `Transformer`) was fine, but estimators were not.
- **Applied Solution**: Duplicate the small `_resolve_backend` helper on `Estimator`
  (lazy-importing the planner to avoid the `planner`↔`base` circular import). Pinned by
  `TestPlannerWiring::test_model_backend_stamped_on_fit`.
- **Useful Analogy (Ma)**: "Same as a mixin placed on only one branch of a sibling
  hierarchy — shared behavior used by both siblings must be defined on each, or on a
  common base they actually share."
- **Files Touched**: `irispark/ml/base.py`, `tests/test_ml_framework.py`.
- **Prevention**: When adding shared behavior to the ML framework, check which base
  classes (`Transformer` vs `Estimator` vs `Model`) actually need it — they are not a
  single inheritance chain.

### 🐛 [2026-08-25] Error: `Pipeline` had no `fit()` and didn't thread stages

- **Context**: Building notebook 23 (LogicalVector & planner example); the pipeline cell
  failed with `AttributeError: 'Pipeline' object has no attribute 'fit'`, then (after
  fixing that) `Field 'X1_S' not found`.
- **Symptom**: `pipe.fit(df)` raised AttributeError; once `Pipeline` inherited `Estimator`,
  the pipeline fit raised SQLCODE -29 because the estimator stage couldn't see the
  `VectorAssembler`'s derived column.
- **Causal Chain (Gu)**:
  - Because `Pipeline` was a `Params` subclass with only `_fit` (no public `fit`) → then
    `pipe.fit(df)` failed → therefore the framework's own `Estimator.fit` contract wasn't
    met by the pipeline.
  - Because `Pipeline._fit` fit each stage on the **original** `df` and never threaded the
    transformed result → then a later estimator stage (e.g. `LogisticRegression`) selected
    a column produced by an earlier stage (`x1_s` from `StandardScaler`) → therefore
    `Field 'X1_S' not found`.
- **Applied Solution**: `Pipeline` now inherits `Estimator` (gaining `fit()`), and
  `Pipeline._fit` threads the transformed DataFrame through stages (`df = stage.transform(df)`
  after each fit/transform). Pinned by notebook 23 running green against live IRIS.
- **Useful Analogy (Ma)**: "Same as a shell pipe — each stage's output must feed the next
  stage's input, or downstream stages reference columns that never existed."
- **Files Touched**: `irispark/ml/pipeline.py`, `scripts/gen_notebooks.py`,
  `notebooks/23_ml_vector_planner.ipynb`.
- **Prevention**: A pipeline is an Estimator and must expose `fit()`; its `_fit` must
  thread the DataFrame through stages so derived columns are visible downstream. Test
  multi-stage pipelines (transformer → transformer → estimator) against live IRIS.

### 🐛 [2026-08-25] Error: `randomSplit` on a withColumn DataFrame → `%ID` not found; R² nested window → -369

- **Context**: Building the ML Analyst series (24–27); notebook 24's `randomSplit` on a
  feature-prepped DataFrame failed, and notebook 25's R² metric failed.
- **Symptom**: `Field '%ID' not found` (SQLCODE -29) when splitting a withColumn-backed
  DataFrame; `A window function cannot be nested in an aggregate` (SQLCODE -369) for R².
- **Causal Chain (Gu)**:
  - Because `randomSplit` splits via `MOD((%ID * seed) ...)` → and `%ID` is a hidden
    column of a physical table, not projected through withColumn subqueries → then
    splitting a feature-prepped (withColumn) DataFrame referenced an invisible `%ID` →
    therefore -29. (Same root cause as the tuning fold fix, but this time fixed at the
    source in `randomSplit`.)
  - Because R² was computed as `SUM((l - AVG(l) OVER ())²)` → and IRIS forbids nesting a
    window function inside an aggregate → therefore -369.
- **Applied Solution**: `randomSplit` now materializes the source to a temp table when it
  has withColumns (so `%ID` is real); R² computes the mean in a subquery
  (`SELECT *, AVG(l) OVER () AS m FROM (...)`) then sums against it. Pinned by
  `TestRegressionEvaluatorR2` and the four MLA notebooks running green.
- **Useful Analogy (Ma)**: "Same as the earlier `%ID`-through-projection lesson — a hidden
  system column never survives a projection; and a window function is a top-level
  construct, not an aggregate argument."
- **Files Touched**: `irispark/dataframe.py`, `irispark/ml/evaluation.py`,
  `scripts/gen_notebooks.py`, `notebooks/24–27_mla_*.ipynb`.
- **Prevention**: Fix `%ID`-dependent operations at the source (materialize) rather than
  in each caller; never nest a window function inside an aggregate — compute the window
  value in a subquery first.

### 🐛 [2026-08-25] Design note: model persistence — learned state vs constructor params

- **Context**: Building model save/load (ml_scope §32); reconstructing fitted models from
  JSON failed for several model types.
- **Symptom**: `TypeError: StandardScalerModel.__init__() got an unexpected keyword
  argument 'mean_'`; `LinearRegressionModel.__init__() missing 'coefficients'/'intercept'`;
  `PipelineModel` had no `save`; `VectorAssembler is not JSON serializable`.
- **Causal Chain (Gu)**:
  - Because model classes store learned state under attribute names that differ from
    their constructor params (`mean_`/`std_` vs `mean`/`std`; `labels_` vs `labels`) →
    then passing learned attrs as constructor kwargs failed → therefore reconstruction
    needed a fallback (construct with params, then `setattr` learned).
  - Because `PipelineModel` is a `Transformer`, not a `Model` → then it was absent from
    the model registry and lacked `save` → therefore it needed explicit registration and
    a `save` method.
  - Because `VectorAssembler` is a `Transformer` (not `Model`) → then the serializer
    didn't handle it → therefore non-Model `Params` stages needed a `__params__` form.
- **Applied Solution**: `_from_state` tries constructor-with-merged-kwargs, falls back to
  params-only + `setattr`; `PipelineModel` registered explicitly + gets `save`; non-Model
  `Params` serialize via `__params__` (class + param map). Pinned by
  `TestModelPersistence` (LinearRegression + 3-stage PipelineModel round-trip).
- **Useful Analogy (Ma)**: "Same as ORM deserialization — the stored field names must map
  back to constructor args, and polymorphic types need a registry."
- **Files Touched**: `irispark/ml/persistence.py`, `irispark/ml/base.py`,
  `irispark/ml/pipeline.py`, `irispark/ml/__init__.py`, `tests/test_ml_supervised.py`.
- **Prevention**: When adding a model class, keep learned-state attribute names aligned
  with constructor params, or ensure the persistence fallback (`setattr`) covers it;
  register any fitted-but-not-`Model` artifact (e.g. PipelineModel) explicitly.

### 🐛 [2026-08-25] Design note: persistence metadata (`backend`, `LogicalVector`) and ctor-arg splitting

- **Context**: Building notebook 28 (model persistence); reloaded models lost `backend` and
  `LogicalVector`, and `LogicalVector` was not JSON-serializable.
- **Symptom**: `AttributeError: ... no attribute 'backend'` after `load()`; `TypeError:
  Object of type LogicalVector is not JSON serializable` on save.
- **Causal Chain (Gu)**:
  - Because `backend`/`logicalVector` are metadata set *after* construction (not ctor
    params) → then the earlier merge-everything-into-ctor approach either rejected them
    (`unexpected keyword argument`) or dropped them (`_SKIP_ATTRS`) → therefore they were
    lost on reload.
  - Because `LogicalVector` is a plain metadata class (not Model/Params) → then the
    serializer didn't handle it → therefore save failed.
- **Applied Solution**: `_from_state` splits learned state into ctor params (via
  `inspect.signature`) vs plain attributes; `LogicalVector` serializes via a
  `__logicalvector__` form (columns/vectorType/metadata). Pinned by
  `TestModelPersistence` and notebook 28 running green.
- **Useful Analogy (Ma)**: "Same as DTO mapping — distinguish constructor fields from
  runtime metadata, and give every serialized type a tagged form."
- **Files Touched**: `irispark/ml/persistence.py`, `scripts/gen_notebooks.py`,
  `notebooks/28_mla_model_persistence.ipynb`.
- **Prevention**: Serialize metadata attributes explicitly, and give every persisted
  object type (Models, Transformer stages, LogicalVector) a tagged JSON form.

### 🐛 [2026-08-25] Design note: EPython/sklearn backend — server-side fit, client data, model persistence

- **Context**: Building the estimator bridge (Phase 8 foundation) — trees/KNN via Embedded Python.
- **Symptom/Findings**: `iris.sql()` server-side table reads are not directly usable from a
  custom EPython function (the returned object is opaque); but **client-passed JSON** into an
  EPython function works cleanly, and sklearn fit + `joblib` persist + reload all work inside
  EPython. The AutoML custom-model dirs live on the server filesystem, so wrapper files must be
  written via an EPython function (base64-encoded source), not client `open()`.
- **Causal Chain (Gu)**: Because the client can't write the server filesystem, and `iris.sql()`
  table reads from arbitrary EPython functions are unreliable → then the working pattern is:
  client sends training data as JSON → EPython fits sklearn → joblib persists to
  `/external/durable/mgr/python/models` → a later EPython function loads + predicts.
- **Applied Solution**: `ensemble.py` generic `_EPythonEstimator` (RandomForest/KNN classifiers
  + regressors); `custom.py` `CustomModelClassifier` writes an `IRISModel` wrapper via a
  base64-payload EPython function. Both verified vs live IRIS.
- **Useful Analogy (Ma)**: "Same as a client-server RPC — pass data as arguments, not by reading
  the server's tables directly; persist state server-side for later calls."
- **Files Touched**: `irispark/ml/ensemble.py`, `irispark/ml/custom.py`, `irispark/ml/planner.py`,
  `irispark/ml/__init__.py`, `tests/test_ml_supervised.py`, `scripts/gen_notebooks.py`.
- **Prevention**: When a backend must run server-side, pass data as JSON function args and
  persist models server-side; test the data-passing + persistence path in a spike before
  building wrappers (this exact spike saved hours).

### 🐛 [2026-08-25] Process error: repeated gen_notebooks.py syntax breakage from inline heredoc patches

- **Context**: Building the CatBoost full-circle notebook; each cell contains Python code
  (with dict access, JSON, file paths) embedded inside a Python string inside a Python source
  file — three layers of quoting simultaneously.
- **Symptom**: `scripts/gen_notebooks.py` broke syntax at least 3 times; notebook 30 failed
  ~6 container runs before passing.
- **Causal Chain (Gu)**:
  - Because each cell spec is a multi-line Python string containing executable Python → then
    every edit requires tracking three escaping layers (source string → JSON → kernel) →
    therefore inline heredoc patches that "seemed right" frequently produced invalid syntax.
  - Because I used `sed`/`grep`/heredoc scripts to patch instead of the edit tool with exact
    strings → then backslash-quote sequences got mangled repeatedly → therefore multiple
    repair cycles were needed.
  - Because I tested only the cell I just edited, not all downstream cells referencing the
    same variables → then a variable rename (`tpred` → `result`) broke an unrelated evaluate
    cell → therefore NameError surfaced only at container execution time.
- **Applied Solution**: Used the edit tool with exact strings from the file for final fixes;
  verified ALL generated cells compile via `compile(src, ...)` after every change; cleaned up
  stray files in the verification clone.
- **Useful Analogy (Ma)**: "Same as editing YAML inside a Jinja template inside a shell
  script — when escaping layers stack, stop patching and rewrite the block cleanly."
- **Files Touched**: `scripts/gen_notebooks.py`, `notebooks/30_mla_catboost_full_circle.ipynb`.
- **Prevention**: For gen_notebooks.py edits: always use the edit tool (not scripts), always
  run `ast.parse` + regenerate + compile-check every generated cell after each change, and
  grep for all references to any renamed variable across the entire NOTEBOOKS entry.

---

## Session & Lifecycle

### 🐛 [2026-08-21] Error: `show()` overrides an existing `LIMIT`

- **Context**: We were running `df.limit(3).show()` in notebook 01 and expected 3 rows.
- **Symptom**: `df.limit(3).show()` printed all 10 rows.
- **Causal Chain (Gu)**:
  - Because `show()` re-applies `limit(n)` with its default `n=10` via `_copy` → then
    `limit_n` was overwritten from 3 → 10 → therefore all rows printed.
- **Applied Solution**: `show()` now caps `n` at the existing `limit_n`
  (`n = min(n, self.limit_n)`), matching PySpark semantics.
- **Useful Analogy (Ma)**: "Similar to an immutable-copy bug — a later transformation
  silently clobbering an earlier one's parameter."
- **Files Touched**: `irispark/dataframe.py`,
  `tests/test_integration.py::TestActions::test_show_respects_existing_limit`.
- **Prevention**: When an action/transformation takes a default parameter that mirrors an
  existing state field, cap/merge rather than overwrite; add a pin test.

### 🐛 [2026-08-14] Error: `timeout=None`/`sslconfig=None` regression

- **Context**: We were trying to build connect kwargs for `IrisParkSession`.
- **Symptom**: Passing `timeout=None`/`sslconfig=None` broke connection construction.
- **Causal Chain (Gu)**:
  - Because the kwargs were built unconditionally → then `None` values were passed to the
    driver → therefore a regression.
- **Applied Solution**: Build connect kwargs conditionally (only include non-None values).
- **Useful Analogy (Ma)**: "Similar to a kwargs-construction bug — only pass what is set."
- **Files Touched**: `irispark/session.py`, `tests/`.
- **Prevention**: Add regression tests for `None` defaults.

### 🐛 [2026-08-14] Error: `getOrCreate` always built a new session

- **Context**: We were trying to implement PySpark's `getOrCreate` semantics.
- **Symptom**: `getOrCreate` always constructed a new session instead of reusing the active one.
- **Causal Chain (Gu)**:
  - Because the method always constructed a new session → then repeated calls leaked
    connections → therefore non-PySpark behavior.
- **Applied Solution**: Return the active session when its config matches; raise `ValueError`
  on mismatch.
- **Useful Analogy (Ma)**: "Similar to a singleton pattern — reuse the active instance."
- **Files Touched**: `irispark/session.py`, `tests/test_integration.py::TestSession`.
- **Prevention**: Test config-match and config-mismatch paths.

### 🐛 [2026-08-14] Error: `na_drop` dropped duplicates instead of NULL rows

- **Context**: We were trying to implement `na.drop`/`na_drop`.
- **Symptom**: Both dropped duplicate rows instead of rows with NULLs.
- **Causal Chain (Gu)**:
  - Because the implementation delegated to the wrong operation → then it removed duplicates
    → therefore wrong semantics.
- **Applied Solution**: Both now delegate to `dropna()` (PySpark semantics: drop rows with NULLs).
- **Useful Analogy (Ma)**: "Similar to a wrong-delegation bug — delegate to the correct
  primitive."
- **Files Touched**: `irispark/dataframe.py`, `tests/test_integration.py::TestDropNa`.
- **Prevention**: Test NULL-row dropping explicitly.

### 🐛 [2026-08-14] Error: `unionByName` stub ignored `allowMissingColumns`

- **Context**: We were trying to implement `unionByName`.
- **Symptom**: The old implementation ignored `allowMissingColumns` and simply called `union()`.
- **Causal Chain (Gu)**:
  - Because the stub ignored the flag → then missing columns were not padded → therefore
    wrong behavior.
- **Applied Solution**: Same-schema frames union directly; same-column-set frames reorder the
  right side; `allowMissingColumns=True` pads missing columns with NULLs; otherwise `ValueError`.
- **Useful Analogy (Ma)**: "Similar to a schema-alignment problem — reorder and pad by name."
- **Files Touched**: `irispark/dataframe.py`, `tests/test_dataframe_extras.py::TestIrisUnionByName`.
- **Prevention**: Test same-schema, reorder, and missing-column paths.

### 🐛 [2026-08-14] Error: IRIS image `latest` silently drifted 2026.1 → 2026.2

- **Context**: We were trying to keep the dev/CI IRIS engine consistent.
- **Symptom**: The `latest` tag silently drifted 2026.1 → 2026.2, which the pinned-dialect
  probes assumed.
- **Causal Chain (Gu)**:
  - Because `latest` is mutable → then the engine changed under us → therefore dialect
    assumptions broke.
- **Applied Solution**: Pinned `intersystemsdc/iris-community:2026.2` in CI and the Makefile.
- **Useful Analogy (Ma)**: "Similar to a floating dependency — pin the version."
- **Files Touched**: `.github/workflows/ci.yml`, `Makefile`, `docker-compose.yml`.
- **Prevention**: Never use `latest`; pin the engine and re-validate on upgrade.

### 🐛 [2026-08-25] Error: `describe()`/`summary()` crashed with `ArrowTypeError` on `show()`

- **Context**: Notebook 01 (`01_dataframe_basics.ipynb`, cell 22) in the new docker-compose
  environment; surfaced because the compose stack is now reproducible end-to-end.
- **Symptom**: `ArrowTypeError: Expected bytes, got a 'int' object` inside
  `pa.RecordBatch.from_arrays` whenever a summary frame was displayed or converted.
- **Causal Chain (Gu)**:
  - Because `describe()`/`summary()` emitted raw typed statistics (COUNT→int, AVG→float,
    MIN of text→str) into the same result column → then `createDataFrame` carried the mixed
    types downstream → therefore pyarrow's type inference (string from first element)
    exploded on the first differently-typed value.
- **Applied Solution**: Match PySpark semantics (all summary cells are strings):
  `CAST(... AS VARCHAR)` on every statistic expression in both methods; percentiles from
  `approxQuantile()` stringified on the Python side. Fix lives at SQL-generation level,
  not in `to_arrow()`.
- **Useful Analogy (Ma)**: "Similar to a CSV export without a schema — every cell becomes
  text so heterogeneous values can share one column."
- **Files Touched**: `irispark/dataframe.py`.
- **Prevention**: When a PySpark method documents uniform output types (e.g., describe,
  summary, explain), mirror that contract exactly before returning user-facing frames;
  add a regression test rendering a mixed int/str/float/None frame through `show()`.

### 🐛 [2026-08-25] Fresh compose instance: EPython `ModuleNotFoundError` → hardcoded `/external` paths → AutoML custom-model hang

- **Context**: Running the MLA notebooks on the new self-contained docker-compose stack
  (fresh IRIS container, auto-init entrypoint) instead of the long-lived dev container.
- **Symptom (3 layers deep)**:
  1. NB30 cell 10: `CB_FIT failed ... No module named 'numpy'` — server-side Python had no ML stack.
  2. After installing deps, NB29: `_IRISPARK_SKLEARN_FIT failed ... PermissionError: '/external'`.
  3. NB29 custom-model cell: hung >3 min; server process in RUN state at ~0 CPU.
- **Causal Chain (Gu)**:
  - Because a fresh instance ships without numpy/sklearn/catboost and EPython imports resolve
    only through paths known to its interpreter → then every Embedded-Python SQL function failed
    until deps were installed into `/usr/irissys/mgr/python` **and** that directory was exposed
    via `PYTHONPATH` (the old env silently relied on system site-packages).
  - Because the EPython function bodies baked `/external/durable/mgr/python/...` from the old
    dev environment → then joblib/model writes crashed with `PermissionError` even after deps
    were fixed; paths must be env-overridable defaults (`IRISPARK_MODEL_DIR`,
    `IRISPARK_AUTOML_DIR`) rooted at the always-writable `mgr/python`.
  - Because IntegratedML's custom-provider discovery blocks server-side on fresh instances
    (`MaxTime` not honored; RUN state, ~0 CPU) while the client polled `%ML.TrainedModel` for up
    to 600s → then one notebook cell froze ~10 minutes before surfacing a TimeoutError.
- **Applied Solution**: entrypoint installs the ML stack idempotently after init; compose sets
  `PYTHONPATH=/usr/irissys/mgr/python`; backend paths now default under `mgr/python` with env
  overrides; `pollTimeout` default 600→90s; notebook 29's custom cell skips gracefully on
  `TimeoutError`, pointing users to the EPython/sklearn bridge.
- **Useful Analogy (Ma)**: "Same as moving into a new apartment — anything bolted to the old
  wall (paths, preinstalled libs) stays behind; ship furniture and floor plan with the tenant."
- **Files Touched**: `docker/iris-entrypoint.sh`, `docker-compose.yml`,
  `irispark/ml/{catboost_backend,ensemble,custom}.py`, `scripts/gen_notebooks.py`,
  `notebooks/29_mla_estimator_bridge.ipynb`.
- **Prevention**: Any new "runs entirely server-side" feature must declare its runtime deps and
  filesystem writes as entrypoint-provisioned + env-overridable, and every long server call gets
  a bounded client-side deadline by default.

### 🐛 [2026-08-25] The AutoML "hang" root cause: `intersystems-iris-automl` was never installed

- **Context**: Follow-up to the compose-provisioning lesson above; the graceful-skip in NB29
  was a band-aid until the actual provider dependency surfaced.
- **Symptom**: `TRAIN MODEL` (custom IRISModel, `%AutoML` provider) blocked server-side on
  fresh instances — RUN state, ~0 CPU — while the identical setup trained in seconds on the
  old dev container.
- **Causal Chain (Gu)**:
  - Because the IntegratedML AutoML provider is implemented by the Python package
    `intersystems-iris-automl` (docs GIML_Configuration_Providers) served from InterSystems'
    own registry (`https://registry.intersystems.com/pypi/simple`) → and the old environment
    had it under `mgr/python` (found via its `requirementsSnapshot.txt`) while fresh compose
    instances never installed it → then provider discovery blocked on the missing module.
  - Because `MaxTime` is measured in **minutes**, defaults to 14400 (10 days), and is only
    honored when `TrainMode:"TIME"` (default is `"SCORE"`) → our `{"MaxTime": 60}` was inert
    either way; nothing bounded the server side.
- **Applied Solution**: entrypoint installs the provider from the InterSystems index and
  verifies `import iris_automl`; compose appends `/usr/irissys/lib/automl` (documented
  package-isolation path) to `PYTHONPATH`; `CustomModelClassifier` sends
  `{"TrainMode": "TIME", "MaxTime": <minutes>}` with default 2. NB29 custom cell now trains
  and predicts for real on a from-scratch stack.
- **Useful Analogy (Ma)**: "Same as blaming a stuck car engine when the fuel line was never
  connected — the skip message treated the symptom."
- **Files Touched**: `docker/iris-entrypoint.sh`, `docker-compose.yml`,
  `irispark/ml/custom.py`, `scripts/gen_notebooks.py`, `notebooks/29_mla_estimator_bridge.ipynb`.
- **Prevention**: When a platform feature delegates to an external runtime component, list
  that component in provisioning from day one and verify its import in the readiness check;
  read the vendor's parameter semantics (units! defaults! preconditions!) before assuming a
  timeout knob does anything.

### 🐛 [2026-08-25] Foreign tables used an invented FDW grammar; official one puts the path in HOST

- **Context**: Notebook 07's `foreign=True` demo on the fresh compose stack.
- **Symptom**: `SQLCODE -1 Invalid SQL statement — HOST expected, IDENTIFIER (OPTIONS) found`
  while preparing `CREATE FOREIGN SERVER ... FOREIGN DATA WRAPPER %SQL.FDW.CSV OPTIONS (...)`.
- **Causal Chain (Gu)**:
  - Because the DDL generator guessed a Postgres-style FDW grammar (wrapper class name +
    OPTIONS clauses) instead of IRIS's documented one → then every file-foreign read failed at
    Prepare. Official grammar (RSQL_createserver / RSQL_createforeigntable): wrapper is plain
    `CSV`; the folder goes in a required `HOST` clause; the table uses `FILE '<name>'` plus
    `USING {"from": {"file": {...}}}` for header/delimiter options.
  - Because client (jupyter container) and IRIS container see different filesystems → even a
    syntactically valid statement would point at a path IRIS cannot read.
- **Applied Solution**: emit the documented grammar; translate `options` into the USING
  `from.file` tree (warn on unknown keys); add `server_path=` to split client-side path (pyarrow
  schema inference) from server-side HOST; compose mounts `./data:/irispark-data` as the shared
  demo volume. Parquet/json now raise a clear ValueError (no standard wrapper).
- **Useful Analogy (Ma)**: "Same as addressing mail with a fictional postal format — the
  postman (parser) stops at the first unknown field no matter how good the address looks."
- **Files Touched**: `irispark/session_iris_extensions.py`, `irispark/read.py`,
  `tests/test_file_foreign_tables.py`, `scripts/gen_notebooks.py`,
  `notebooks/{07_read_write,19_de_federation_foreign_tables}.ipynb`, `docker-compose.yml`.
- **Prevention**: For any vendor DDL, copy the Synopsis verbatim from the reference page and pin
  a live round-trip test (create → query → drop) before shipping the API.

### 🐛 [2026-08-25] JDBC foreign tables: named SQL Gateway connection + empty `properties` NPE

- **Context**: `read.jdbc()` / `register_jdbc_foreign_table()` emitted an invented grammar
  (`FOREIGN DATA WRAPPER %SQL.FDW.XDBC OPTIONS (url ..., user ..., password ..., driver ...)`)
  that IRIS 2026.2 rejects. Official grammar (RSQL_createserver / RSQL_createforeigntable):
  `FOREIGN DATA WRAPPER JDBC CONNECTION '<named connection>'` and
  `CREATE FOREIGN TABLE ... SERVER <srv> TABLE '<remote table>'`.
- **Symptom**: `SQLCODE -1 Invalid SQL statement (XDBC)`; after switching to the official DDL,
  `SQLCODE -237 Schema import for foreign table did not return column metadata` with a gateway
  `java.lang.NullPointerException ... ConcurrentHashMap.putVal`.
- **Causal Chain (Gu)**:
  - Because a foreign JDBC server requires a *named* SQL Gateway connection stored in
    `%Library.sys_SQLConnection` (in %SYS) → the library must INSERT one (works via plain SQL).
  - Because inserting the `properties` column with an **empty string** `''` makes the JDBC
    gateway NPE during schema import → omit the column entirely unless a value is provided
    (a NULL properties is fine, an empty string is not).
- **Applied Solution**: `_ensure_jdbc_connection()` INSERTs the named connection (JDBC type,
  omitting `properties` when empty); DDL uses `JDBC CONNECTION '<name>'` +
  `SERVER <srv> TABLE '<remote>'`; schema is discovered via a `SELECT ... LIMIT 0` probe after
  create; `writer.jdbc` and `create_foreign_table_from_query` updated to the same grammar.
- **Useful Analogy (Ma)**: "Same as a car that starts only with the fuel gauge present — an
  empty optional field still poisons the startup path."
- **Files Touched**: `irispark/session_iris_extensions.py`, `irispark/read.py`,
  `irispark/writer.py`, `tests/test_foreign_tables.py`, `tests/test_jdbc.py` (removed — obsolete
  sqlalchemy path), `scripts/gen_notebooks.py`, notebooks 14/19.
- **Prevention**: When a DDL feature delegates to an IRIS service (SQL Gateway), exercise the
  full create → import → query loop live, and never send an empty-string value for an optional
  column the vendor treats as a sentinel.
