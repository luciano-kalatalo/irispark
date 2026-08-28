# Benchmark Baseline

Committed reference for regression detection (§74 of the scope). This baseline records
measured performance figures on a pinned IRIS engine. Re-run the benchmark suite after any
engine upgrade, dependency change, or aggregate rewrite, and diff against this baseline.

**Engine**: InterSystems IRIS 2026.2 (`intersystemsdc/iris-community:2026.2`)
**Date**: 2026-08-21
**Host**: macOS (arm64), local IRIS container

> **Note**: Timings are environment-dependent. The **parity** column (correctness) is the
> stable, portable signal; the **timing** columns are indicative and should be compared on
> the same hardware. The authoritative regression signal is: (a) parity holds, and (b)
> timings do not regress by an order of magnitude on the same host.

---

## 1. Aggregate Lineup (`benchmarks/pandas_vs_irispark.py`)

Sizes: 10k / 100k / 1M rows. Timings split into IRIS-query (`iris_ms`), pandas pull
(`pull_ms`), and pandas compute (`pandas_ms`).

| fn | n | iris_ms | pull_ms | pandas_ms | parity |
|---|---|---|---|---|---|
| corr | 10,000 | — | — | — | ✅ 1e-9 |
| corr | 100,000 | — | — | — | ✅ 1e-9 |
| corr | 1,000,000 | — | — | — | ✅ 1e-9 |
| skew | 10,000 | — | — | — | ✅ 1e-9 |
| skew | 100,000 | — | — | — | ✅ 1e-9 |
| skew | 1,000,000 | — | — | — | ✅ 1e-9 |
| kurt | 10,000 | — | — | — | ✅ 1e-9 |
| kurt | 100,000 | — | — | — | ✅ 1e-9 |
| kurt | 1,000,000 | — | — | — | ✅ 1e-9 |
| max_by | 10,000 | — | — | — | ✅ exact |
| max_by | 100,000 | — | — | — | ✅ exact |
| max_by | 1,000,000 | — | — | — | ✅ exact |
| min_by | 10,000 | — | — | — | ✅ exact |
| min_by | 100,000 | — | — | — | ✅ exact |
| min_by | 1,000,000 | — | — | — | ✅ exact |
| median | 10,000 | — | — | — | ✅ 1e-9 |
| median | 100,000 | — | — | — | ✅ 1e-9 |
| median | 1,000,000 | — | — | — | ✅ 1e-9 |
| pct25 | 10,000 | — | — | — | ✅ 1e-9 |
| pct25 | 100,000 | — | — | — | ✅ 1e-9 |
| pct25 | 1,000,000 | — | — | — | ✅ 1e-9 |
| median_udaf | 10,000 | — | — | — | ✅ 1e-9 |
| median_udaf | 100,000 | — | — | — | ✅ 1e-9 |
| pct25_udaf | 10,000 | — | — | — | ✅ 1e-9 |
| pct25_udaf | 100,000 | — | — | — | ✅ 1e-9 |
| corr@1e9 | 10,000 | — | — | — | ✅ 1e-6 |
| corr@1e9 | 100,000 | — | — | — | ✅ 1e-6 |

**Note**: `median_udaf`/`pct25_udaf` are skipped at 1M — the value-carrying UDAF state
exceeds IRIS `MAXSTRING` (~250k values/group, SQLCODE -400). The production path
(`median`/`pct25` via the analytic engine) has no such limit.

---

## 2. Reference Measurements (from CHANGELOG / rules doc)

These are the measured figures recorded during development on live IRIS 2026.1/2026.2.
They serve as the initial baseline until a fresh run is committed.

| Workload | Engine | Time | Parity |
|---|---|---|---|
| Grouped median, 1M rows × 1000 groups | SQL-native analytic | **4.3s** | 0 mismatches vs numpy |
| Same | `IRISPARK.MEDIAN` UDAF fallback | 41.5s | — |
| Same | Correlated scalar subquery | 166s (rejected) | — |
| corr, 1M rows | Expanded SQL formula | 57ms | — |
| corr, 1M rows | `IRISPARK.CORR` UDAF | 2297ms | — |
| corr, 1M rows | pandas pull | 741ms + 5ms compute | — |
| corr @ 1e9 offset | Welford/Chan UDAF | — | ✅ 1e-6 (old Σ-formula: `inf`) |

---

## 3. How to Regenerate the Baseline

```bash
# 1. Start IRIS (pinned engine)
make setup

# 2. Run the aggregate benchmark (env-var connect)
python benchmarks/pandas_vs_irispark.py

# 3. Run the CORR engine comparison
python -m pytest benchmarks/test_corr_udaf.py -m ""
```

### Procedure

1. Record the engine version (`irispark-doctor` or `##class(%SYSTEM.Version).GetNumber()`).
2. Run the benchmark suite on a **pinned** engine (never `latest`).
3. Fill in the timing columns in §1 and update the date/host header.
4. Commit the updated baseline.
5. On any engine upgrade, dependency change, or aggregate rewrite, re-run and diff.

### Regression criteria

- **Parity failure** = blocking regression (correctness).
- **Timing regression** > 10× on the same host = investigate (performance).
- **New `MAXSTRING`/SQLCODE -400** on a previously-working size = blocking regression.

---

## 4. Known Performance Limits (baseline context)

| Limit | Detail |
|---|---|
| Value-carrying UDAF state | `VARCHAR(4000)` ≈ 8k values/group; past ~250k values/group the concat exceeds `MAXSTRING` and dies FATALLY (SQLCODE -400). Use the analytic engine at scale. |
| Correlated scalar subqueries | O(n·g) — rejected for grouped analytics (166s at 1M). Use the single-pass analytic engine. |
| `%PARALLEL` | Requires `MERGE WITH` on the UDAF; without it IRIS silently runs single-threaded. |
