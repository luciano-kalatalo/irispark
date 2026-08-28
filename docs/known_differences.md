# Known Differences — IrisPark vs PySpark

This document consolidates every place where IrisPark deliberately differs from PySpark.
It is the authoritative reference for "why does my PySpark code behave differently here?".

Two categories:

1. **Behavioral deviations** — the same call runs but returns a different value/type.
2. **Unsupported APIs** — the call does not exist or is out of scope.

For the full function-by-function compatibility matrix, see
[`compatibility.md`](compatibility.md). For the migration classification, see
[`migration.md`](migration.md).

---

## 1. Behavioral Deviations (same call, different result)

These are pinned in `scripts/parity.py` (`KNOWN_DEVIATIONS`) and verified against live
PySpark 3.5.8. They are deliberate: IRIS cannot represent the value, or its dialect
semantics differ.

| Function | IrisPark | PySpark | Why |
|---|---|---|---|
| `round(x, 2)` at exact `.5` double boundary | `2.67` (rounds the binary double `2.67499…`) | `2.68` (rounds the decimal string `2.675`) | IRIS `ROUND` operates on the exact binary double; Spark on the decimal string. Difference of 0.01 on `2.675`. |
| `pow`/`power` domain errors (`POWER(0,-1)`, `POWER(neg, fractional)`) | `NULL` | `±Inf` / `NaN` | IRIS cannot represent Inf/NaN; the unguarded form is a **fatal SQLCODE -400** (query dies). Guarded to NULL. |
| `regexp_extract` no-match | `NULL` | `''` (empty string) | IRIS `VARCHAR` maps the empty string to NULL. |
| `StatFunctions.cov(col1, col2)` with no valid observation pair | `None` | `0.0` | Consistent with `corr`; no valid pair → no result. |
| `pyspark.pandas.io` module functions | require explicit `session=` or active session | read a global default session | IrisPark sessions are transient (`close()` clears the active session); every `io` function accepts an explicit `session=` parameter. Without it, raises `RuntimeError` when no session exists. |

### Non-round-trippable values

- **`NaN` inputs** — IRIS temp tables crash on the `NAN` literal; NaN inputs are not
  round-trippable. Parity expectations drop NaN-input rows (`nanvl` NaN-input rows excluded).

### Float comparison tolerance

- Parity uses ULP-tolerant comparison (1e-12/1e-9) to cover composition drift in
  `cosh`/`log1p`. Numeric strings/ints are normalized (`width_bucket` int-vs-float, IRIS
  `ROUND` string returns). `" 00:00:00"` is truncated from IRIS date strings (`trunc`).

---

## 2. Unsupported APIs

### 2.1 No valid SQL mapping in IRIS (verified via dialect probes)

Hard dialect walls — not scope decisions. Workarounds exist only at ObjectScript/EPython level.

| Missing | Evidence |
|---|---|
| `bitwiseNOT` / `bitwiseAND` / `bitwiseOR` / `bitwiseXOR`, `shiftLeft`, `shiftRight`, `bit_count`, `getbit` | Verified unsupported by the IRIS SQL catalog |
| `try_cast` | Same probe note |
| `Column.getField` / `getItem`, `dropFields` / `withField`, `outer` join | Same probe note |
| `groupingSets` | Verified unsupported on IRIS 2026.2 (`ROLLUP`/`CUBE`/`GROUPING SETS` parse as function/field references; 2026.1 rejected with SQLCODE -29) |

### 2.2 Type-system gap: arrays / maps / structs

IRIS columns are flat scalars. `ArrayType`/`MapType`/`StructType` exist in `irispark/types.py`
only as schema metadata — there is no value-level SQL mapping for collection columns.

| Missing | Why |
|---|---|
| `array`, `array_contains`, `array_distinct`, `array_max/min/position/remove/repeat/sort`, `sort_array`, `map`, `map_keys`, `map_values`, `size`, `posexplode`, `struct`, `named_struct`, `create_map` | Spark collection semantics need array/map/struct column types; IRIS's `%DynamicArray`/`%DynamicObject` is a per-value object model, not a column type. |
| Higher-order: `transform`, `filter`, `reduce`, `zip_with`, `exists`, `forall` | Same dependency; would require an EPython bridge. |

### 2.3 JSON family

| Missing | Why |
|---|---|
| `from_json`, `to_json`, `get_json_object`, `json_tuple`, `schema_of_json`, `from_csv` | IRIS stores JSON as text/`%DynamicObject`; no SQL-function family mapped or probed. JSON access is a known EPython lane — not wired into the SQL generator. |

### 2.4 Dialect gaps with trivial workarounds (priority, not feasibility)

IRIS lacks these built-ins; implementable as ObjectScript helper UDFs or simple SQL
emulation. Parked as low demand.

| Group | Missing |
|---|---|
| Math | `asin`, `acos`, `atan`, `atan2`, `cbrt`, `expm1`, `log1p`, `log2`, `hypot`, `hex`, `unhex`, `bin`, `sinh`, `cosh`, `tanh`, `sec*` family |
| String | `substring_index`, `translate`, `overlay`, `octet_length` |
| Date/time | `make_date`, `make_timestamp`, `to_timestamp` (IRIS has `DATEDIFF`/`DATEADD` but not Spark-exact semantics; needs parity probes) |
| Conditional/misc | `assert_true`, `if` / `iff` |

### 2.5 Meaningless single-node (by design)

| Missing | Why |
|---|---|
| `input_file_name`, `input_file_block_*`, `spark_partition_id`, `monotonically_increasing_id`, `randn`-per-partition semantics | No files, no partitions, no worker identities — Spark per-node identity functions have no IRIS analogue. |
| Streaming / MLlib / Delta / Iceberg / CarbonData | Out of scope — delegated to sklearn/xgboost/lightgbm + IRIS Interoperability. |

### 2.6 I/O readers without IRIS backing

| Missing | Why |
|---|---|
| `read_delta`, `read_orc`, `read_excel`, `read_html`, `read_pickle` | No IRIS or dependency backing; client-side pandas/openpyxl/deltalake only. |
| Module-level `to_*` writers | Covered by `df.write.*` / `df.to_pandas()`. |

---

## 3. Status-correction footnote

Functions earlier believed missing but actually covered: `rlike` (emulated as
`regexp_extract(col, pattern, 0) != ''`), `regexp_extract` / `regexp_replace` (native IRIS
2026), `nanvl`, `ilike` (via `UPPER ... LIKE UPPER(...)`), `find_in_set`, `elt`,
`char_length`, `bit_length`, `width_bucket`, `uniform`, `format_string`, `printf`,
`parse_url`, `from_utc_timestamp`, `to_utc_timestamp`.

---

## 4. Closing the gaps

- **Cheapest lane: §2.4** — a few ObjectScript helper UDFs each, parity-testable (pandas
  reference + SQL compile probes, per the standard UDAF/UDF test matrix).
- **§2.2 + §2.3 need a decision first** — `%DynamicArray`/`%DynamicObject` mapping or an
  Embedded Python bridge (`LANGUAGE PYTHON` — partially supported already in
  `irispark/udf.py`; SCOPE2 lists full EPython UDF registration as v2.0).
- **§2.1 and §2.5 are walls by verification/design** — re-probe §2.1 only when IRIS adds
  the operators.
