# Troubleshooting

Common issues, their causes, and fixes. Each entry follows the causal chain (Gu) pattern
from `LESSONS_LEARNED.md`.

---

## 1. Connection & Session

### `getOrCreate()` raises `ValueError`
- **Cause**: The builder config does not match the active session's config.
- **Fix**: `getOrCreate()` reuses the active session only if all config keys match
  (`host`, `port`, `namespace`, `username`, `password`, `timeout`, `sslconfig`). Close the
  active session or align the config.

### Connection fails with `timeout`/`sslconfig` errors
- **Cause**: `None` values forwarded to the driver.
- **Fix**: `timeout` and `sslconfig` are optional; when `None` they are omitted from the
  connection kwargs. Only pass them when set.

### Session startup crashes on UDF/UDAF install
- **Cause**: A reserved-word aggregate name (e.g. `FIRST`) or a missing UDC path.
- **Fix**: Aggregates are renamed (`AGG_FIRST`); the UDC path is auto-probed and skips
  gracefully when absent. Set `IRISPARK_UDC_PATH` if the probe finds no candidate.

---

## 2. SQL Generation

### `SQLCODE -14` syntax error on string predicates
- **Cause**: Raw-SQL string predicates quoted as literals (e.g. `'id > 2'`).
- **Fix**: Use `count_if`/`bool_and`/`bool_or` with the `_predicate_expr()` path, which
  embeds strings verbatim. Prefer Column predicates.

### `SQLCODE -1` / `-12` on exponentiation
- **Cause**: `^` or `**` used in an aggregate expression.
- **Fix**: Use `POWER(expr, n)`.

### `SQLCODE -378` datatype mismatch on `COALESCE`
- **Cause**: Numeric literal fallback not cast to the anchor column's type.
- **Fix**: `coalesce`/`ifnull`/`nvl` cast numeric fallbacks to the anchor column's IRIS type.

### `SQLCODE -25` on `IS NULL` in SELECT list
- **Cause**: IRIS rejects `IS NULL` on arbitrary expressions in the SELECT list.
- **Fix**: Wrap in `CASE WHEN … IS NULL`.

### `SQLCODE -359` intermittent on UDF calls
- **Cause**: Schema-qualified UDF lookups are exact-case.
- **Fix**: Render UDF call sites lowercase.

---

## 3. Aggregates & UDAFs

### Query dies FATALLY with `SQLCODE -400` on median/percentile
- **Cause**: Value-carrying UDAF state exceeds IRIS `MAXSTRING` (~3.6MB) past ~250k
  values/group.
- **Fix**: Use the SQL-native analytic engine (`functions.median`/`percentile`), which has
  no state-size limits. The UDAF is validated up to 100k values/group.

### `%PARALLEL` runs single-threaded
- **Cause**: The UDAF has no `MERGE WITH`.
- **Fix**: `MERGE WITH` is mandatory for `%PARALLEL`. Also verify hint placement:
  `FROM %PARALLEL <table>` (the post-table position is a parse error on 2026.2).

### corr returns `inf`/`NaN` at large offsets
- **Cause**: Σ-formula catastrophic cancellation.
- **Fix**: Use the Welford/Chan online algorithm (the deployed `IRISPARK.CORR`).

### A worker spins at 100% CPU forever (client hangs)
- **Cause**: An unparenthesized arithmetic expression in a `While` condition in a
  SQL-embedded ObjectScript body.
- **Fix**: `docker restart iris` to recover. Then hoist arithmetic into a `Set` or
  parenthesize it. `While` conditions must contain bare variables/literals and comparisons
  only.

---

## 4. Parity & Data

### `round(x, 2)` differs from Spark at exact `.5` boundaries
- **Cause**: IRIS rounds the binary double; Spark rounds the decimal string.
- **Fix**: Documented deviation (2.675 → 2.67 vs 2.68). See [Known Differences](known_differences.md).

### `pow`/`power` domain errors kill the query
- **Cause**: IRIS cannot represent Inf/NaN; `POWER(0,-1)` is a fatal SQLCODE -400.
- **Fix**: Guarded to NULL (documented deviation).

### Non-ASCII strings round-trip incorrectly
- **Cause**: The driver/IRIS data path stores UTF-8 byte-per-char.
- **Fix**: Parity is scoped to ASCII for V1; documented limitation for a future driver
  charset-negotiation fix.

### `NaN` inputs crash temp tables
- **Cause**: IRIS temp tables crash on the `NAN` literal.
- **Fix**: NaN inputs are not round-trippable; drop them from expectations.

---

## 5. Environment

### IRIS image silently drifted versions
- **Cause**: Using `latest` (mutable).
- **Fix**: Pin `intersystemsdc/iris-community:2026.2` and re-validate on any upgrade.

### `make setup` fails after docker-compose removal
- **Cause**: Old targets referenced the deleted `docker-compose.yml`.
- **Fix**: `make setup`/`make clean` now use the same `docker run` recipe as CI.

### `irispark-doctor` reports `CHECK FAILED`
- **Cause**: IRIS connection, version, CPU flags (AVX/AVX2/BMI/BMI2), or columnar/vector
  support issue.
- **Fix**: Check the IRIS connection and version, Python version, and platform
  architecture. See the [Deployment Guide](deployment_guide.md).
