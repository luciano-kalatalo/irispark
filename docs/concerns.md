# IrisPark — Weak Points Deep Analysis

**Audience:** InterSystems IRIS partners evaluating IrisPark for client offerings.
**Date:** 2026-08-11
**Based on:** Full codebase review of v0.9.x (irispark/, tests/, CHANGELOG.md, SCOPE.md)

---

## 1. Single-node only (no distributed execution)

**Code evidence:**
- `session.py:96` — exactly one `iris.connect()` call. No connection pool.
- `context.py:13-32` — `parallelize()` splits data into Python lists. "Partitions" are just list slices.
- `dataframe.py:375-383` — `coalesce()` and `repartition()` are literal no-ops. They return a copy and append a lineage entry.
- Zero references to "shard", "cluster", "worker", or "distributed" anywhere in the codebase.

**Opinion:** This is the right call for v1.0, but it's the single biggest ceiling on the product. IRIS itself supports ECP (Enterprise Cache Protocol) for distributed query execution across nodes. IrisPark doesn't expose a read-replica connection string. For a partner pitch: "This runs on a single IRIS instance. For most credit scoring workloads (<100M rows), that's fine. If you need horizontal scale, you'd use IRIS sharding/ECP at the database layer — IrisPark doesn't need to know about it." But you need a concrete benchmark to back that up. Without one, a skeptical architect will kill the deal.

**Verdict:** Real limitation, but mitigatable with IRIS ECP at the DB layer. Needs a benchmark whitepaper.

---

## 2. IRIS lock-in (only works with InterSystems IRIS)

**Code evidence:**
- `session.py:8` — `import iris` is unconditional. No abstraction layer.
- `column.py:33,40,113,120,124,128` — `%EXACT` collation prefix on 6 string comparison methods. Pure IRIS syntax.
- `sql_generator.py:125,160,198,326,534,595,599` — `TOP` instead of `LIMIT`, `%ID` pseudo-column. Both IRIS-specific.
- `irispark/sql/udf/` — 16 ObjectScript UDFs (~430 lines) using `##class(%SYSTEM.Encryption)`, `##class(%Regex.Matcher)`, `$ZCONVERT`, `$ZCRC`, `LIST()` aggregation. All IRIS-only.
- `functions.py:326,329,519,523,527` — `LIST(col)` for collect_list, median, percentile. IRIS-specific (standard SQL uses `ARRAY_AGG`).

**Opinion:** This is simultaneously the biggest weakness AND the entire point of the product. IrisPark exists *because* IRIS is the target. The lock-in is the value proposition — "you already have IRIS, now you get a DataFrame API on top of it." For an IRIS partner, this isn't a bug, it's the sales pitch. The risk is that a prospect says "we might migrate off IRIS someday." The answer is: "Then you'd need a different tool. IrisPark is for teams committed to IRIS." Don't try to be database-agnostic — that would dilute the IRIS-specific value (vector search, `%EXACT` collation, ObjectScript UDFs). Lean into it.

**Verdict:** Not a weakness for an IRIS partner. It's the moat.

---

## 3. Row-oriented storage (IRIS is row-store, not columnar)

**Code evidence:**
- `dataframe.py:574-624` — `_ensure_schema()` does `SELECT * FROM table LIMIT 0` to get all column metadata. No column pruning.
- `sql_generator.py:765-855` — `_build_select()` emits `*` or user-specified columns. No analysis of which columns are actually consumed downstream.
- `dataframe.py:669-687` — `to_arrow()` converts ALL columns to Arrow arrays. No subsetting.
- SCOPE.md line 523: "Row-oriented storage for columnar scans — Bank credit scoring datasets typically <100M rows. IRIS indexing covers common query patterns. Monitor and benchmark."

**Opinion:** This is a real concern for wide tables (100+ columns) where you only need 3. But it's IRIS's problem, not IrisPark's. IRIS has bitmap indexes and columnar projection in its SQL engine. IrisPark doesn't add overhead — it generates `SELECT col1, col2, col3` when the user calls `.select("col1", "col2", "col3")`. The `*` is only used when the user hasn't selected specific columns. For a partner pitch: "IrisPark generates the SQL you'd write by hand. IRIS optimizes it. If you `select` 3 columns from a 100-column table, only those 3 are read from disk." Verify this with an IRIS `EXPLAIN` plan to have ammunition.

**Verdict:** Minor concern. IRIS handles this at the engine level. IrisPark doesn't make it worse.

---

## 4. No streaming / CDC

**Code evidence:**
- Zero streaming code. Zero references to "stream", "CDC", "kafka", "change data capture" in any Python file.
- `read.py` handles batch Parquet/CSV/JDBC ingestion only.
- SCOPE.md line 464: "Streaming / real-time ingestion — Batch-oriented."
- Planned for v3.0 (checklist.md line 333).

**Opinion:** This is a gap for real-time use cases, but it's honest about it. The architecture doc says "real-time scoring uses static Gold tables in IRIS" — meaning you batch-train, batch-update the Gold table, and serve predictions from a pre-computed table. That's how most "real-time" ML systems work in practice. True streaming (Kafka → IRIS → feature computation → model inference) is a v3.0 feature and a different product category. For credit scoring, batch is usually fine — scores are recomputed nightly. Don't overpromise streaming. If a client needs it, IRIS has Interoperability for HL7/Kafka/etc., and you'd integrate at that layer, not in IrisPark.

**Verdict:** Honest gap. Not a dealbreaker for the target use case (batch credit scoring). Be upfront.

---

## 5. No built-in ML algorithms

**Code evidence:**
- `irispark/ml/feature.py` — 5 feature transformers (186 lines total). All follow sklearn `fit()/transform()` pattern but generate SQL.
- `VectorAssembler` (line 50-73): Concatenates columns with `|| ',' ||`. Output is a comma-separated string — not a numeric vector. Workaround because IRIS has no native vector type.
- `StringIndexer` (line 76-113): `SELECT DISTINCT ... ORDER BY` for fit, `CASE WHEN` chains for transform.
- `OneHotEncoder` (line 116-149): `CASE WHEN` per category, concatenated with `||`.
- `StandardScaler` (line 152-186): `AVG()`/`STDDEV()` via SQL, arithmetic in transform.
- `QuantileDiscretizer` (line 12-47): `approxQuantile()` for boundaries, `CASE WHEN` for binning.
- No algorithms. No training. No MLflow integration (planned v2.0).

**Opinion:** This is fine. The transformers are feature engineering — they prepare data for sklearn/xgboost/lightgbm. That's exactly what PySpark's ML transformers do too. The gap is that PySpark has MLlib (algorithms that run distributed). IrisPark doesn't need that because sklearn/xgboost run fine on a single machine for the dataset sizes IRIS handles. The real missing piece is **MLflow integration** (v2.0). Without it, you can't tell a client "track your experiments, register models, serve them." That's the Databricks-killer feature. Prioritize MLflow over everything else in v2.0.

**Verdict:** Feature engineering is solid. Missing MLflow is the real gap. Prioritize it.

---

## 6. SQL generation complexity

**Code evidence:**
- `sql_generator.py` — 892 lines, single file, ~25 methods, no modularization.
- 8+ distinct nesting patterns: withColumn chains (one subquery per column), withColumnRenamed + filters, CUBE/ROLLUP (UNION ALL of 2^N GROUP BYs), unpivot/stack (UNION ALL per column), union with post-ops, dropDuplicates (ROW_NUMBER OVER), semi/anti join (WHERE EXISTS), sample (LCG in WHERE).
- Known bugs fixed across versions: double-alias (v0.8.6), HAVING routing (v0.8.7), union post-ops silently dropped (v0.8.3), rename propagation (v0.8.2, v0.9.2), multi-join dropping first join (v0.8.4).
- Zero TODO/FIXME/HACK comments. Zero tests for the SQL generator in isolation — all testing is integration-level.

**Opinion:** This is the highest technical risk in the codebase. The SQL generator is a single 892-line file with no abstraction (no dialect interface, no AST, no visitor pattern). Every new feature adds another conditional branch. The subquery nesting is deep — `withColumn` + `withColumnRenamed` + `join` + `filter` + `orderBy` can produce 4-5 levels of nesting. IRIS can handle it, but debugging a `SQLCODE -29` at 5 levels of nesting is painful. The fact that there are zero unit tests for the generator (only integration tests) means regressions are caught late. For a partner selling this, the risk is: a client writes a complex chain of operations, gets a cryptic IRIS SQL error, and you can't easily debug it. Invest in: (1) splitting sql_generator.py into smaller modules, (2) adding pure-SQL-generation unit tests (no IRIS needed), (3) a `df.to_sql()` that pretty-prints the SQL with indentation.

**Verdict:** Highest technical debt. Needs refactoring before it becomes unmaintainable. Not a sales blocker but a support risk.

---

## 7. Temp tables for createDataFrame (row-by-row INSERT)

**Code evidence:**
- `session.py:252-254` — for each row in the data: `self.sql(f'INSERT INTO "{tbl}" VALUES ({vals})')`. N round-trips for N rows.
- `read.py:193-201` — same pattern for Parquet/CSV ingestion.
- `read.py:153-159` — same for JDBC.
- IRIS doesn't support multi-row `VALUES (...), (...)` (confirmed in CHANGELOG v0.8.1).
- `session.py:241` — type inference samples only 10 rows.
- SCOPE.md line 483: "Acceptable for MVP. Arrow-backed in-memory deferred."

**Opinion:** This is the most visible performance problem. If a data scientist does `spark.createDataFrame(pandas_df)` with 100,000 rows, that's 100,000 separate INSERT statements. Each one is a network round-trip to IRIS. This will be **seconds to minutes** of overhead before any analysis starts. The fix is to use IRIS's bulk load capabilities (SQL `IMPORT` or ObjectScript `%SQL.Import`), or better yet, keep the data in Arrow/Python memory and only push to IRIS when SQL operations are needed. For a partner demo, keep `createDataFrame` datasets small (<1000 rows) or pre-load data into IRIS tables. This is the first thing a technical evaluator will notice.

**Verdict:** Critical performance gap for demos and POCs. Fix before showing to clients.

---

## 8. Immutability is convention-only

**Code evidence:**
- `writer.py:50-75` — `saveAsTable()` with `overwrite` does `DROP TABLE` + `CREATE TABLE AS SELECT`. Destructive.
- `writer.py:77-99` — `insertInto()` with `overwrite` does `DELETE FROM` + `INSERT INTO`. Destructive.
- No WORM, no object lock, no audit logging, no soft delete, no versioning.
- SCOPE.md line 520: "Immutability is convention, not hardware-enforced — Read-only archived schemas + maximum IRIS audit logging + quarterly Parquet snapshots to S3 with Object Lock for Basel III proof."

**Opinion:** This is a regulatory risk, not a technical one. For Basel III / IFRS 9, you need to prove that the data used to train a model hasn't been tampered with. The SCOPE.md mitigation is reasonable: use IRIS audit logging + periodic S3 Parquet snapshots with Object Lock. But none of that is implemented in IrisPark. For a bank client, you'd need to show: (1) IRIS audit logging is enabled and immutable, (2) S3 snapshots are automated and WORM-locked, (3) the lineage tracking in IrisPark (`df.lineage()`) provides the chain of custody. The lineage feature exists and is solid (`dataframe.py:560-572`). The S3 snapshot automation doesn't. Build a simple `df.write.snapshot("s3://...")` that writes a Parquet with a timestamp and optionally applies S3 Object Lock. That's a 2-day feature that materially improves the compliance story.

**Verdict:** Regulatory gap, not technical. Lineage tracking helps. Add S3 snapshot automation for compliance demos.

---

## 9. OLTP/OLAP contention

**Code evidence:**
- Nothing. Zero resource governance, zero query prioritization, zero workload isolation.
- `session.py:96` — single connection. No read/write split.
- SCOPE.md line 522: "Schedule heavy scans outside peak hours. IRIS read replica if contention proves real."

**Opinion:** This is an operational concern, not a product gap. IRIS has resource governance at the database level (per-process memory limits, query timeouts). IrisPark doesn't need to implement this — it just needs to not make it worse. The real answer for a client is: "Use an IRIS read replica for analytics queries. IrisPark connects to the replica. Your loan origination system connects to the primary. Zero contention." This is a 1-line config change (different host/port). The fact that IrisPark doesn't have a built-in "read replica" config option is a minor gap — you can just create two sessions. For a partner pitch, have a slide showing the architecture: Primary IRIS (OLTP) ← → Read Replica IRIS (OLAP via IrisPark). That's a standard pattern.

**Verdict:** Operational concern solved by IRIS read replicas. Not a product gap. Document the pattern.

---

## 10. Small ecosystem / single maintainer

**Code evidence:**
- `.github/workflows/ci.yml` — single CI workflow (lint + test-offline + test-online).
- `CONTRIBUTING.md` — 83 lines, basic.
- No issue templates, no PR templates, no CODEOWNERS, no SECURITY.md, no governance model.
- No Discord/Slack/forum links.
- 24 releases in ~12 weeks — high velocity, single author pattern.

**Opinion:** This is the reality of an early-stage open-source project. For an IRIS partner, this is actually an advantage — you can position yourself as the expert who provides commercial support, training, and customization on top of the open-source core. The risk is that the original author stops maintaining it. Mitigations: (1) the codebase is well-structured and documented, (2) it's pure Python with no exotic dependencies, (3) the test suite is comprehensive (774 tests). A competent Python developer could take over maintenance. For enterprise clients, offer a support SLA. That's your value-add as a partner.

**Verdict:** Normal for early-stage OSS. Partner opportunity: offer commercial support.

---

## 11. No Spark Connect

**Code evidence:**
- Zero references to Spark Connect, gRPC, or remote execution.
- SCOPE.md line 463: "Spark Connect compatibility — Not included."
- Planned for v4.0+ (long-term future).

**Opinion:** Irrelevant for the target use case. Spark Connect is for connecting BI tools (Tableau, Power BI) to Spark. IrisPark's target user is a data scientist in a Jupyter notebook, not a BI tool. If a client needs BI connectivity, they'd connect Tableau directly to IRIS via ODBC/JDBC — no IrisPark needed. Don't waste time on this.

**Verdict:** Not a weakness. Out of scope by design.

---

## 12. Proprietary driver (intersystems-irispython)

**Code evidence:**
- `session.py:8` — `import iris` is hard-coded. No abstraction.
- `requirements.txt:1` — `intersystems-irispython` is a hard dependency.
- `pyproject.toml:24` — same.
- No `ConnectionFactory`, no `Dialect` interface, no adapter pattern.

**Opinion:** Same as #2 (IRIS lock-in). This is the product. The `intersystems-irispython` driver is the official InterSystems Python driver. It's proprietary, yes, but it's the supported way to connect to IRIS from Python. For an IRIS partner, this is fine — your clients already have IRIS licenses, which include the driver. The risk is if InterSystems changes the driver API. Mitigation: the driver is stable and has been for years. The `iris.connect()` API is unlikely to change.

**Verdict:** Not a weakness for IRIS partners. It's the standard driver.

---

## 13. No Delta Lake / Iceberg

**Code evidence:**
- Zero references to Delta, Iceberg, time travel, ACID transactions, or snapshots.
- `writer.py:50-75` — `saveAsTable()` creates plain IRIS tables. No format versioning.
- `read.py:73-89` — `read.parquet()` reads files into IRIS temp tables. No table format.
- SCOPE.md line 481: "No Iceberg or Delta Lake — Medallion implemented as IRIS tables with date partitioning."
- Planned for v4.0+ (optional, for 50TB+ historical data).

**Opinion:** This is a philosophical choice, not a gap. The whole point of IrisPark is that IRIS IS the storage layer. You don't need Delta Lake because IRIS has ACID transactions, indexes, and SQL. The Medallion architecture (Bronze → Silver → Gold) is implemented as IRIS tables with date partitioning — that's simpler and sufficient for bank-scale data. Delta Lake would add complexity (another format to manage, another catalog) with zero benefit if your data already lives in IRIS. For a client who says "but Databricks uses Delta Lake," the answer is: "Databricks needs Delta Lake because S3 is not a database. IRIS is a database. You don't need a table format on top of a database." This is a strong differentiator if you frame it right.

**Verdict:** Not a weakness. It's a philosophical advantage. Frame it as "IRIS is the database — you don't need a table format on top of a database."

---

## 14. Pre-1.0 status (v0.9.x)

**Code evidence:**
- `pyproject.toml:7` — `version = "0.9.2"` (but CHANGELOG shows 0.9.4 — version mismatch).
- `pyproject.toml:13` — classifier `"Development Status :: 4 - Beta"`.
- 24 releases in ~12 weeks. API has been additive-only, no breaking changes.
- SCOPE.md line 549: "v1.0 (Current — v0.9.1) — Core Platform — Status: Production-ready for basic analytics."
- 774 tests passing.

**Opinion:** The version number is misleading. This is functionally a 1.0 product. The API is stable, the test coverage is solid, and the documentation is thorough. The "0.9.x" label is conservative. For a partner selling this: (1) fix the version mismatch (pyproject.toml says 0.9.2, CHANGELOG says 0.9.4), (2) bump to 1.0.0, (3) update the classifier to "Production/Stable". The product is ready. The version number is the only thing holding it back from a "1.0" perception. Clients see "0.9" and think "beta, not ready." That's a marketing problem, not a technical one.

**Verdict:** Marketing gap. Bump to 1.0.0. The product is ready.

---

## Priority Action Items

| Priority | Issue | Effort | Impact |
|----------|-------|--------|--------|
| **P0** | `createDataFrame` row-by-row INSERT performance | 2-3 days | Demos will feel slow |
| **P0** | Version bump to 1.0.0 + fix version mismatch | 5 minutes | "0.9" scares enterprise buyers |
| **P1** | SQL generator refactoring (modularize, add unit tests) | 1 week | Supportability |
| **P1** | MLflow integration (v2.0) | 2 weeks | The Databricks-killer feature |
| **P1** | S3 snapshot automation for compliance | 2 days | Basel III story |
| **P2** | Read replica documentation/pattern | 1 day | OLTP/OLAP contention answer |
| **P2** | Performance benchmark whitepaper | 3 days | Answer "does it scale?" |
| **P3** | IRIS diagnostics (`df.iris.*`) | 1 week | Nice-to-have differentiator |

---

## Partner Pitch Summary

> "IrisPark gives your data scientists a PySpark-compatible API on IRIS. No Spark cluster, no data duplication, no ETL to S3. It's not Spark — it's better for IRIS workloads because it pushes everything down to IRIS SQL. Here's the benchmark. Here's the compliance story. Here's the MLflow integration. You already own IRIS — this unlocks it for your data team."
