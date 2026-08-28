# Performance Guide & Benchmark Methodology

This guide documents IrisPark's performance model, the benchmark suite, and the
methodology for measuring and validating performance. It also serves as the committed
baseline reference for regression detection (§74 of the scope).

---

## 1. Performance Model

IrisPark's core principle is **"move computation to the data, rather than moving data to
the computation"** (`rules_functions_aggregations.md` §24). All filtering, aggregation,
grouping, sorting, and joins push down to IRIS SQL. Python is an analytical extension, not
the default execution engine.

The execution preference order is:

```
Native SQL → ObjectScript UDF → ObjectScript UDAF → Embedded Python
```

IrisPark does **not** build a query optimizer — IRIS already has a world-class cost-based
optimizer. IrisPark's job is to generate clean SQL and expose IRIS capabilities.

---

## 2. Engine Selection & Performance Trade-offs

| Engine | When to use | Performance notes |
|---|---|---|
| **Native SQL** | Any operation IRIS supports natively | Fastest; runs entirely in the IRIS optimizer |
| **Expanded SQL formula** | corr/covar/median/percentile at scale | Recommended production route for aggregates (e.g. corr: 57ms at 1M vs 2297ms UDAF) |
| **ObjectScript UDAF** | Reference-correct aggregates | ~40x slower than expanded SQL at 1M rows; fixed-size state is safe at any size |
| **SQL-native analytic engine** | median/percentile/quantile | 4.3s at 1M×1000 groups (vs 41.5s UDAF fallback, 166s correlated subquery) |
| **Embedded Python** | Scientific/ML/vectorized | Batch-oriented; avoid per-row Python |

### Key measured results (live IRIS 2026.1/2026.2)

| Workload | Engine | Time |
|---|---|---|
| Grouped median, 1M rows × 1000 groups | SQL-native analytic | **4.3s**, 0 mismatches vs numpy |
| Same | `IRISPARK.MEDIAN` UDAF fallback | 41.5s |
| Same | Correlated scalar subquery | 166s (rejected) |
| corr, 1M rows | Expanded SQL formula | 57ms |
| corr, 1M rows | `IRISPARK.CORR` UDAF | 2297ms |
| corr, 1M rows | pandas pull | 741ms + 5ms compute |

---

## 3. Benchmark Suite

The `benchmarks/` directory contains:

| File | Purpose |
|---|---|
| `pandas_vs_irispark.py` | Aggregate lineup (CORR/SKEW/KURT/MEDIAN/PERCENTILE/MAX_BY/MIN_BY) vs pandas over `[10k, 100k, 1M]` rows, plus a corr 1e9-offset stability case |
| `test_corr_udaf.py` | CORR engine comparison (UDAF vs expanded SQL vs pandas pull) at 10k/100k/1M |
| `test_ingestion.py` | Data ingestion benchmarks |
| `test_overhead.py` | Per-query overhead benchmarks |
| `test_storage.py` | Storage benchmarks |
| `test_volume.py` | Volume benchmarks |
| `generate_data.py` | Data generator |

### Running the aggregate benchmark

```bash
# Standalone (env-var connect)
python benchmarks/pandas_vs_irispark.py

# From a notebook
%run -i benchmarks/pandas_vs_irispark.py
```

Connection honors `IRIS_HOST` / `IRIS_PORT` / `IRIS_NAMESPACE` / `IRIS_USERNAME` /
`IRIS_PASSWORD`.

### Engine split in the benchmark

- **corr/skew/kurt/max_by/min_by** run the raw ObjectScript UDAFs (fixed-size state, safe
  at any size).
- **median/pct25** run the **production path** (`functions.median`/`percentile`, expanded
  into the analytic engine — no state-size limits).
- **median_udaf/pct25_udaf** reference rows run the value-carrying UDAFs directly, **capped
  at 100k rows** — past ~250k values in one group the state exceeds IRIS `MAXSTRING`
  (~3.6MB) and the query dies fatally (SQLCODE -400).

---

## 4. Benchmark Methodology

### Metrics recorded

- Total execution time
- CPU consumption
- Memory consumption
- Throughput (rows processed per second)
- Number of cross-engine calls
- Type conversion overhead
- Parallelization capability

### Numerical validation

Statistical results are compared against recognized references:

- `numpy.corrcoef()` / `pandas.Series.corr()` for correlation
- `pandas.Series.skew()` / `kurt()` for moments
- `pandas.Series.median()` / `quantile()` for distribution

Tolerances: **1e-9** for standard parity; **1e-6** for the corr 1e9-offset stability case
(pandas itself loses precision at that scale).

### Discriminating test matrix

A rewrite is only provable if a test exists that the old implementation fails. The standard
matrix:

- pandas parity across dataset shapes (normal, skewed, uniform, heavy-tailed, mixed-scale,
  small n, constant, NULLs, bimodal)
- NULL-pair skipping
- constant column → NULL
- `n < 2` → NULL
- empty input → NULL
- `%PARALLEL` ≡ serial on every shape
- one magnitude-stress case (e.g. the `large_offset` 1e9 base that caught the corr `inf` bug)

---

## 5. Regression Detection (§74)

The benchmark suite detects regressions between:

- IRIS versions
- IrisPark versions
- Python versions
- operating systems
- storage modes

**Baseline practice**: record benchmark output on a pinned IRIS engine (e.g. 2026.2) as a
committed reference. Re-run after any engine upgrade, dependency change, or aggregate
rewrite, and diff against the baseline. Never use `latest` for the IRIS image — it drifts
and invalidates baselines.

---

## 6. Known Performance Limits

| Limit | Detail |
|---|---|
| Value-carrying UDAF state | `VARCHAR(4000)` ≈ 8k values/group; past ~250k values/group the concat exceeds `MAXSTRING` and dies FATALLY (SQLCODE -400). Use the analytic engine at scale. |
| Correlated scalar subqueries | O(n·g) — rejected for grouped analytics (166s at 1M). Use the single-pass analytic engine. |
| Single-node only | By design; IRIS Sharding handles distribution at the SQL layer. |
| `%PARALLEL` | Requires `MERGE WITH` on the UDAF; without it IRIS silently runs single-threaded. |
