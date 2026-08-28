# SCOPE2.md — IrisPark v1.0 Waterfall Project Plan

**Document type:** Waterfall-style project scope, requirements, and phase plan
**Target release:** v1.0.0
**Current version:** 0.9.4
**Date:** 2026-08-11
**Methodology:** Waterfall (sequential phases with formal gates)

---

## Table of Contents

1. [Project Definition](#1-project-definition)
2. [Requirements Phase](#2-requirements-phase)
3. [Design Phase](#3-design-phase)
4. [Implementation Phase](#4-implementation-phase)
5. [Verification Phase](#5-verification-phase)
6. [Release Phase](#6-release-phase)
7. [Post-1.0 Roadmap](#7-post-10-roadmap)
8. [Appendix: Architecture Reference](#8-appendix-architecture-reference)

---

## 1. Project Definition

### 1.1 Product Identity

**IrisPark** is a DataFrame-native analytical interface for InterSystems IRIS. It brings the PySpark DataFrame programming model to IRIS while pushing computation into IRIS SQL, allowing data teams to work in a familiar Python interface without introducing a separate Spark data platform.

### 1.2 Strategic Thesis

```
DataFrame API → Logical Plan → IRIS SQL → IRIS engine (indexes, columnar storage/indexes, sharding, optimizer) → results
```

**PySpark compatibility is the on-ramp. IRIS awareness is the moat.**

IrisPark does NOT attempt to recreate Spark's distributed execution engine, MLlib, Structured Streaming, or Spark Connect. Its role is to provide a familiar Python/DataFrame programming model while delegating execution and optimization to IRIS.

### 1.3 Core Principles

1. **DataFrame-first experience** — Fluent PySpark-like API
2. **IRIS-first execution** — All operations push down to IRIS SQL
3. **Lazy evaluation** — Transformations build a DAG, actions trigger execution
4. **SQL transparency** — `df.to_sql()` shows generated SQL, `df.explain()` shows IRIS plan
5. **Simplicity over feature parity** — Focus on common analytics patterns
6. **Native IRIS extensions** — Expose IRIS capabilities through DataFrame API

### 1.4 v1.0 Success Criteria

The v1.0 release is successful when:

1. A data scientist can query IRIS tables using a PySpark-compatible DataFrame API without writing manual SQL
2. `createDataFrame(pandas_df)` with 100K rows completes in under 2 seconds (not minutes)
3. CSV/Parquet/JDBC ingestion uses batch loading (not row-by-row INSERT)
4. Generated SQL is transparent, debuggable (`to_sql(pretty=True)`), and regression-tested
5. Columnar storage and columnar indexes are accessible through the API
6. Benchmarks prove SQL pushdown overhead is <5% vs hand-written IRIS SQL
7. Benchmarks prove columnar storage improves analytical query performance
8. All 774+ existing tests pass; 30+ new SQL generator unit tests pass
9. Version metadata is consistent; governance artifacts exist (SECURITY.md, known limitations)
10. API is stable with no breaking changes since 0.9.4

### 1.5 Explicit Non-Goals for v1.0

| Non-goal | Rationale |
|----------|-----------|
| Distributed Python runtime | IRIS Sharding handles query distribution at the SQL layer |
| MLlib-equivalent algorithms | Delegate to sklearn/xgboost/lightgbm |
| Spark Connect / gRPC | No current strategic justification |
| Structured Streaming / CDC | Batch-oriented; streaming is v3.0+ |
| Delta Lake / Iceberg | IRIS tables are the storage layer |
| MLflow integration | v1.1 (post-1.0) |
| Multi-database support | IRIS-only by design |

---

## 2. Requirements Phase

**Entry criteria:** Project definition approved. Stakeholders aligned on v1.0 scope.

**Exit criteria:** All requirements documented, reviewed, and baselined. No open requirement questions.

### 2.1 Functional Requirements

#### FR-1: Bulk Data Ingestion

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-1.1 | `createDataFrame(pandas_df)` shall use batch INSERT (100 rows per statement) instead of row-by-row INSERT | P0 | Performance gap identified in code review |
| FR-1.2 | `read.csv(path)` shall use batch INSERT for loading data into IRIS temp tables | P0 | Same as FR-1.1 |
| FR-1.3 | `read.parquet(path)` shall use batch INSERT for loading data into IRIS temp tables | P0 | Same as FR-1.1 |
| FR-1.4 | `read.jdbc(url, dbtable)` shall register an IRIS Foreign Server + Foreign Table instead of copying rows locally; SQLAlchemy may be used for metadata introspection | P0 | Same as FR-1.1 |
| FR-1.5 | Batch INSERT shall fall back to row-by-row INSERT if batching fails | P1 | Graceful degradation |
| FR-1.6 | `read.load_data(path)` shall use IRIS `LOAD DATA FROM FILE` for server-side files when JVM is available | P1 | IRIS-native bulk path |

#### FR-2: SQL Generator Testability

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-2.1 | SQL generator shall have a dedicated unit test suite (`tests/test_sql_generator.py`) with minimum 30 tests | P0 | Core execution engine has zero unit tests |
| FR-2.2 | SQL generator tests shall run without an IRIS connection (pure string assertions) | P0 | Enables fast CI feedback |
| FR-2.3 | `df.to_sql(pretty=True)` shall return formatted SQL with newlines and indentation | P0 | Debugging and support |
| FR-2.4 | `df.explain()` shall use pretty-printed SQL in its logical plan output | P1 | Quality of life |
| FR-2.5 | SQL generator tests shall cover: SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, JOIN (6 types), UNION, Window, withColumn, withColumnRenamed, Distinct, Drop, Sample, Pivot, Unpivot, CUBE, ROLLUP, fillna, dropna, edge cases | P0 | Comprehensive coverage |

#### FR-3: Columnar Storage Awareness

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-3.1 | `DataFrameWriter.storageType("columnar" | "row")` shall configure the storage layout for `saveAsTable()` | P1 | IRIS supports `WITH STORAGETYPE = COLUMNAR` |
| FR-3.2 | `saveAsTable()` shall append `WITH STORAGETYPE = COLUMNAR` to the DDL when `storageType("columnar")` is set | P1 | DDL generation |
| FR-3.3 | `df.iris.createColumnarIndex(column)` shall execute `CREATE COLUMNAR INDEX` on the table | P1 | IRIS columnar indexes on row tables |
| FR-3.4 | `df.iris.createBitmapIndex(column)` shall execute `CREATE BITMAP INDEX` on the table | P2 | IRIS bitmap indexes |
| FR-3.5 | `df.iris.explain()` shall return the IRIS execution plan as structured output (list of strings) | P1 | Diagnostics |
| FR-3.6 | `df.iris.tableStats()` shall return row count for the table | P2 | Diagnostics |

#### FR-4: Version and Governance Hygiene

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-4.1 | `pyproject.toml` version shall match `CHANGELOG.md` latest entry | P0 | Current mismatch (0.9.2 vs 0.9.4) |
| FR-4.2 | `SECURITY.md` shall exist with security reporting process | P0 | Governance artifact |
| FR-4.3 | Known limitations shall be documented in README or a dedicated section | P0 | Transparency |
| FR-4.4 | API stability: no breaking changes to public API since 0.9.4 | P0 | Backward compatibility |

#### FR-5: Benchmark Suite

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-5.1 | Benchmark A: Compare IrisPark-generated SQL execution time vs hand-written IRIS SQL for identical queries | P0 | Prove pushdown overhead |
| FR-5.2 | Benchmark D: Compare row storage vs columnar storage vs columnar indexes for analytical queries | P0 | Prove columnar value |
| FR-5.3 | Benchmark F: Compare row-by-row INSERT vs batch INSERT vs LOAD DATA for 100K rows | P0 | Prove ingestion improvement |
| FR-5.4 | All benchmarks shall be reproducible via `python -m pytest benchmarks/` | P0 | Reproducibility |
| FR-5.5 | Benchmark results shall be logged to `benchmarks/results/` | P1 | Audit trail |

### 2.2 Non-Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| NFR-1 | `createDataFrame(pandas_df)` with 100K rows shall complete in <2 seconds | P0 |
| NFR-2 | IrisPark SQL generation overhead shall be <5% of total query execution time | P0 |
| NFR-3 | All 774 existing tests shall continue to pass | P0 |
| NFR-4 | SQL generator unit tests shall complete in <1 second | P1 |
| NFR-5 | No new dependencies shall be added to `requirements.txt` | P2 |

### 2.3 Constraints

| ID | Constraint |
|----|------------|
| C-1 | Must maintain backward compatibility with existing IrisPark 0.9.x API |
| C-2 | Must work with `intersystemsdc/iris-community:2026.2` Docker image (pinned; formerly `:latest` which drifted 2026.1 → 2026.2) |
| C-3 | Must support Python 3.10, 3.11, 3.12 |
| C-4 | `LOAD DATA` support requires JVM on IRIS server (optional, not blocking) |

### 2.4 Requirements Sign-off

| Role | Name | Date | Status |
|------|------|------|--------|
| Product Owner | — | — | ⬜ |
| Technical Lead | — | — | ⬜ |
| QA Lead | — | — | ⬜ |

---

## 3. Design Phase

**Entry criteria:** All requirements baselined. No open requirement questions.

**Exit criteria:** Design documents reviewed and approved. All design decisions documented.

### 3.1 Architecture Overview (v1.0 Target State)

```
┌─────────────────────────────────────────┐
│          Python / Jupyter Notebook       │
├─────────────────────────────────────────┤
│           IrisParkSession                │
│  ┌───────────────────────────────────┐  │
│  │ IrisDataFrame API (lazy, fluent)  │  │
│  │  - select, filter, groupBy, agg   │  │
│  │  - join, union, window            │  │
│  │  - withColumn, orderBy, limit     │  │
│  │  - to_sql(pretty=True)            │  │
│  │  - explain()                      │  │
│  │  - df.iris.* (NEW)               │  │
│  └──────────────┬────────────────────┘  │
│                 │                        │
│  ┌──────────────▼────────────────────┐  │
│  │ SQLGenerator                      │  │
│  │  - DAG → IRIS SQL translation     │  │
│  │  - _format_sql(pretty) (NEW)      │  │
│  │  - Unit-tested (NEW)              │  │
│  └──────────────┬────────────────────┘  │
│                 │                        │
│  ┌──────────────▼────────────────────┐  │
│  │ Ingestion Layer (REFACTORED)      │  │
│  │  - _batch_insert() (NEW)          │  │
│  │  - createDataFrame (batch)        │  │
│  │  - read.csv/parquet/jdbc (batch)  │  │
│  │  - read.load_data() (NEW, opt)    │  │
│  └──────────────┬────────────────────┘  │
│                 │                        │
│  ┌──────────────▼────────────────────┐  │
│  │ DataFrameWriter (ENHANCED)        │  │
│  │  - storageType("columnar") (NEW)  │  │
│  │  - saveAsTable + STORAGETYPE      │  │
│  └──────────────┬────────────────────┘  │
│                 │                        │
│  ┌──────────────▼────────────────────┐  │
│  │ IrisExtensions (NEW)              │  │
│  │  - explain()                      │  │
│  │  - createColumnarIndex()          │  │
│  │  - createBitmapIndex()            │  │
│  │  - tableStats()                   │  │
│  └───────────────────────────────────┘  │
└─────────────────┬───────────────────────┘
                  │ iris.connect()
                  ▼
┌─────────────────────────────────────────┐
│       InterSystems IRIS SQL Engine      │
│  ┌───────────────────────────────────┐  │
│  │ Cost-Based Optimizer              │  │
│  │  - Predicate pushdown             │  │
│  │  - Join reordering                │  │
│  │  - Index selection                │  │
│  ├───────────────────────────────────┤  │
│  │ Storage Layer                     │  │
│  │  - Row storage (default)          │  │
│  │  - Columnar storage (NEW via API) │  │
│  │  - Columnar indexes (NEW via API) │  │
│  │  - Bitmap indexes (NEW via API)   │  │
│  ├───────────────────────────────────┤  │
│  │ Scalability (transparent)         │  │
│  │  - Sharding (query distribution)  │  │
│  │  - ECP (distributed caching)      │  │
│  │  - Reporting async mirrors        │  │
│  └───────────────────────────────────┘  │
└─────────────────┬───────────────────────┘
                  │ Arrow RecordBatch
                  ▼
┌─────────────────────────────────────────┐
│     Pandas / Polars / Dask (interop)    │
└─────────────────────────────────────────┘
```

### 3.2 Component Design — Batch Insert

**Module:** `irispark/session.py`
**New method:** `IrisParkSession._batch_insert()`

```
Input: table_name (str), columns (list[str]), rows (list[tuple]), batch_size (int = 100)
Output: None (side effect: rows inserted into IRIS table)

Algorithm:
  1. If rows is empty, return immediately
  2. Quote all column names with double-quotes
  3. For each batch of batch_size rows:
     a. Build semicolon-separated INSERT statements
     b. Execute as single sql() call
  4. If any batch fails, log warning and fall back to row-by-row

Error handling:
  - Empty rows: no-op
  - Batch failure: fall back to row-by-row INSERT for that batch
  - Connection error: propagate exception
```

**Affected callers:**
- `IrisParkSession.createDataFrame()` — replace row loop with `_batch_insert()`
- `Read._table_to_iris_dataframe()` — replace row loop with `_batch_insert()`
- `Read.jdbc()` — replace row loop with `_batch_insert()`

### 3.3 Component Design — SQL Generator Test Suite

**Module:** `tests/test_sql_generator.py` (new file)
**Test pattern:** Pure unit tests, no IRIS connection required

```
Test class structure:
  TestBasicSelect        — SELECT * / columns / selectExpr
  TestFilter             — WHERE single / AND / OR / Column objects
  TestGroupByAgg         — dict agg / expr agg / HAVING / UDF aggs
  TestOrderByLimit       — ASC / DESC / TOP
  TestJoin               — inner / left / right / full / semi / anti / multi
  TestWithColumn         — single / chained
  TestUnion              — bare / with post-ops (filter, orderBy, limit)
  TestWindow             — row_number / rank / lag
  TestDistinctDrop       — distinct / drop / dropDuplicates
  TestSample             — sample / randomSplit
  TestPivotUnpivot       — pivot / unpivot / stack
  TestCubeRollup         — CUBE / ROLLUP
  TestFillnaDropna       — fillna / dropna
  TestWithColumnRenamed  — basic / +filter / +agg
  TestEdgeCases          — empty / NULL / reserved words / injection prevention

Mock strategy:
  - Use a mock IrisParkSession that never calls iris.connect()
  - IrisDataFrame can be instantiated directly with constructor params
  - Assert df.to_sql() output matches expected SQL string
```

### 3.4 Component Design — `to_sql(pretty=True)`

**Module:** `irispark/sql_generator.py`
**New function:** `_format_sql(sql: str) -> str`

```
Algorithm:
  1. Insert newline before: SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY
  2. Indent subqueries (content between parentheses) by 2 spaces
  3. Indent JOIN ... ON clauses by 2 spaces
  4. Indent AND in WHERE/HAVING clauses

Example output:
  SELECT region, SUM(amount) AS sum_amount
  FROM (
    SELECT * FROM vendas
    WHERE year = 2025
  ) AS _sub
  GROUP BY region
  HAVING SUM(amount) > 1000
  ORDER BY sum_amount DESC
```

### 3.5 Component Design — Columnar Storage Awareness

**Module:** `irispark/writer.py` (modified)
**New method:** `DataFrameWriter.storageType(type: str) -> DataFrameWriter`

```
State: self._storage_type: str | None = None

Valid values: "row", "columnar" (case-insensitive)
Invalid values: raise ValueError

Effect on saveAsTable():
  - If self._storage_type is set, append " WITH STORAGETYPE = {type}" to CREATE TABLE DDL
  - If not set, no change from current behavior (defaults to row storage)
```

**Module:** `irispark/iris_extensions.py` (new file)
**New class:** `IrisExtensions`

```
Properties:
  _df: IrisDataFrame (set in __init__)

Methods:
  explain() -> list[str]
    - Runs EXPLAIN on the DataFrame's generated SQL
    - Returns list of plan lines

  createColumnarIndex(column: str) -> None
    - Generates: CREATE COLUMNAR INDEX idx_{table}_{column}_col ON {table}({column})
    - Executes via session.sql()

  createBitmapIndex(column: str) -> None
    - Generates: CREATE BITMAP INDEX idx_{table}_{column}_bmp ON {table}({column})
    - Executes via session.sql()

  tableStats() -> dict
    - Runs: SELECT COUNT(*) FROM {table}
    - Returns {"row_count": int}
```

**Module:** `irispark/dataframe.py` (modified)
**New property:** `IrisDataFrame.iris -> IrisExtensions`

```python
@property
def iris(self) -> IrisExtensions:
    return IrisExtensions(self)
```

### 3.6 Component Design — Benchmark Suite

**Directory:** `benchmarks/` (new)

```
benchmarks/
  __init__.py
  conftest.py          # session fixture, data generation helpers
  generate_data.py     # Synthetic data generator (numeric, string, date columns)
  test_overhead.py     # Benchmark A
  test_storage.py      # Benchmark D
  test_ingestion.py    # Benchmark F
  test_volume.py       # Benchmark B (post-1.0)
  test_width.py        # Benchmark C (post-1.0)
  results/             # Output directory for benchmark logs
    .gitkeep
```

**Benchmark A — Overhead:**
```
For each query pattern (aggregation, join, filter+group):
  1. Generate hand-written IRIS SQL
  2. Generate equivalent IrisPark DataFrame code
  3. Execute both, measure elapsed time (3 runs each, take median)
  4. Assert result equivalence
  5. Assert IrisPark overhead < 5%
```

**Benchmark D — Storage:**
```
1. Generate 1M rows of synthetic data (10 numeric columns, 5 string columns)
2. Create three tables:
   a. t_row: WITH STORAGETYPE = ROW
   b. t_col: WITH STORAGETYPE = COLUMNAR
   c. t_row_idx: WITH STORAGETYPE = ROW + columnar index on numeric columns
3. Run 5 analytical queries on each (aggregation, filter+aggregate, groupBy, join, window)
4. Measure elapsed time for each query on each storage type
5. Report speedup of columnar vs row
```

**Benchmark F — Ingestion:**
```
1. Generate 100K rows of synthetic data in pandas DataFrame
2. Measure time for:
   a. Current row-by-row INSERT (baseline)
   b. Batch INSERT (100 rows per statement)
   c. LOAD DATA FROM FILE (if JVM available)
3. Report speedup of batch vs row-by-row
```

### 3.7 Design Decisions

| Decision | Rationale |
|----------|-----------|
| Batch INSERT over LOAD DATA for createDataFrame | LOAD DATA requires JVM and server-side files; batch INSERT works everywhere with no dependencies |
| Semicolon-separated statements over true batching | IRIS supports multiple statements per cursor.execute(); simpler than executemany with parameter binding |
| Batch size of 100 | Balances statement string length (~10KB per batch) with round-trip reduction (100x) |
| `df.iris` namespace over top-level functions | Keeps IRIS-specific extensions namespaced; doesn't pollute the PySpark-compatible surface |
| `storageType()` on writer over `saveAsTable(storage_type=...)` | Fluent builder pattern matches existing `mode()` API |
| Pure string assertion tests over integration tests | Fast (<1s), no IRIS dependency, catches regressions immediately in CI |
| Pretty SQL via string formatting over AST | Pragmatic; full AST/IR refactoring is post-1.0 (P2) |

### 3.8 Design Sign-off

| Role | Name | Date | Status |
|------|------|------|--------|
| Technical Lead | — | — | ⬜ |
| Architect | — | — | ⬜ |

---

## 4. Implementation Phase

**Entry criteria:** Design documents approved. Development environment ready.

**Exit criteria:** All code written, all unit tests passing, all benchmarks implemented.

### 4.1 Phase 0: Version Hygiene

**Duration:** 30 minutes
**Dependencies:** None
**Deliverables:**
- `pyproject.toml` version updated to `0.9.4`
- `CHANGELOG.md` entry for `0.9.5` created

**Tasks:**

| ID | Task | File | Owner | Status |
|----|------|------|-------|--------|
| IMP-0.1 | Update `version` in `pyproject.toml` from `0.9.2` to `0.9.4` | `pyproject.toml:7` | — | ⬜ |
| IMP-0.2 | Add `## [0.9.5] — 2026-08-11` section to CHANGELOG | `CHANGELOG.md` | — | ⬜ |

**Verification:**
- `grep version pyproject.toml` returns `0.9.4`
- `head -5 CHANGELOG.md` shows `[0.9.5]`

---

### 4.2 Phase 1: Bulk Ingestion

**Duration:** 2-3 days
**Dependencies:** Phase 0 complete
**Deliverables:**
- `IrisParkSession._batch_insert()` method
- Refactored `createDataFrame()`, `read.csv()`, `read.parquet()`, `read.jdbc()`
- Ingestion performance test

**Tasks:**

| ID | Task | File | Owner | Status |
|----|------|------|-------|--------|
| IMP-1.1 | Implement `_batch_insert()` method on `IrisParkSession` | `irispark/session.py` | — | ⬜ |
| IMP-1.2 | Refactor `createDataFrame()` to use `_batch_insert()` | `irispark/session.py:252-254` | — | ⬜ |
| IMP-1.3 | Refactor `_table_to_iris_dataframe()` to use `_batch_insert()` | `irispark/read.py:191-201` | — | ⬜ |
| IMP-1.4 | Refactor `read.jdbc()` to use `_batch_insert()` | `irispark/read.py:152-159` | — | ⬜ |
| IMP-1.5 | Add fallback logic: if batch fails, retry row-by-row | `irispark/session.py` | — | ⬜ |
| IMP-1.6 | Add ingestion performance test (1000 rows, assert <1s) | `tests/test_integration.py` | — | ⬜ |

**Verification:**
- `python -m pytest tests/test_integration.py -v -k "createDataFrame"` — all pass
- Manual test: `createDataFrame(pandas_df)` with 100K rows completes in <2s

---

### 4.3 Phase 2: SQL Generator Test Suite

**Duration:** 3-4 days
**Dependencies:** Phase 0 complete (can run in parallel with Phase 1)
**Deliverables:**
- `tests/test_sql_generator.py` with 30+ tests
- `_format_sql()` function in `sql_generator.py`
- `to_sql(pretty=True)` on `IrisDataFrame`
- Enhanced `explain()` output

**Tasks:**

| ID | Task | File | Owner | Status |
|----|------|------|-------|--------|
| IMP-2.1 | Create `tests/test_sql_generator.py` with mock session | `tests/test_sql_generator.py` (new) | — | ⬜ |
| IMP-2.2 | Implement `TestBasicSelect` (3 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.3 | Implement `TestFilter` (4 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.4 | Implement `TestGroupByAgg` (5 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.5 | Implement `TestOrderByLimit` (3 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.6 | Implement `TestJoin` (5 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.7 | Implement `TestWithColumn` (2 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.8 | Implement `TestUnion` (2 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.9 | Implement `TestWindow` (3 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.10 | Implement `TestDistinctDrop` (3 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.11 | Implement `TestSample` (2 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.12 | Implement `TestPivotUnpivot` (2 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.13 | Implement `TestCubeRollup` (2 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.14 | Implement `TestFillnaDropna` (2 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.15 | Implement `TestWithColumnRenamed` (3 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.16 | Implement `TestEdgeCases` (5 tests) | `tests/test_sql_generator.py` | — | ⬜ |
| IMP-2.17 | Implement `_format_sql()` in `sql_generator.py` | `irispark/sql_generator.py` | — | ⬜ |
| IMP-2.18 | Add `pretty` parameter to `IrisDataFrame.to_sql()` | `irispark/dataframe.py:662-663` | — | ⬜ |
| IMP-2.19 | Enhance `explain()` to use pretty SQL | `irispark/dataframe.py:548-558` | — | ⬜ |

**Verification:**
- `python -m pytest tests/test_sql_generator.py -v` — 30+ tests pass, no IRIS required
- `df.to_sql(pretty=True)` produces indented, readable SQL
- `df.explain()` shows formatted logical plan

---

### 4.4 Phase 3: Columnar Storage Awareness

**Duration:** 2-3 days
**Dependencies:** Phase 0 complete (can run in parallel with Phases 1-2)
**Deliverables:**
- `DataFrameWriter.storageType()` method
- `IrisExtensions` class in `irispark/iris_extensions.py`
- `df.iris` property on `IrisDataFrame`

**Tasks:**

| ID | Task | File | Owner | Status |
|----|------|------|-------|--------|
| IMP-3.1 | Add `_storage_type` field and `storageType()` method to `DataFrameWriter` | `irispark/writer.py` | — | ⬜ |
| IMP-3.2 | Modify `saveAsTable()` to append `WITH STORAGETYPE` clause | `irispark/writer.py:74` | — | ⬜ |
| IMP-3.3 | Create `irispark/iris_extensions.py` with `IrisExtensions` class | `irispark/iris_extensions.py` (new) | — | ⬜ |
| IMP-3.4 | Implement `IrisExtensions.explain()` | `irispark/iris_extensions.py` | — | ⬜ |
| IMP-3.5 | Implement `IrisExtensions.createColumnarIndex()` | `irispark/iris_extensions.py` | — | ⬜ |
| IMP-3.6 | Implement `IrisExtensions.createBitmapIndex()` | `irispark/iris_extensions.py` | — | ⬜ |
| IMP-3.7 | Implement `IrisExtensions.tableStats()` | `irispark/iris_extensions.py` | — | ⬜ |
| IMP-3.8 | Add `iris` property to `IrisDataFrame` | `irispark/dataframe.py` | — | ⬜ |
| IMP-3.9 | Export `IrisExtensions` from `irispark/__init__.py` | `irispark/__init__.py` | — | ⬜ |

**Verification:**
- `df.write.storageType("columnar").saveAsTable("t")` creates a columnar table
- `df.iris.createColumnarIndex("col")` creates a columnar index
- `df.iris.explain()` returns execution plan lines

---

### 4.5 Phase 4: Benchmark Suite

**Duration:** 3-4 days
**Dependencies:** Phases 1-3 complete (needs batch INSERT, columnar storage, pretty SQL)
**Deliverables:**
- `benchmarks/` directory with 3 benchmark files
- Synthetic data generator
- Benchmark results logged to `benchmarks/results/`

**Tasks:**

| ID | Task | File | Owner | Status |
|----|------|------|-------|--------|
| IMP-4.1 | Create `benchmarks/` directory structure | `benchmarks/` (new) | — | ⬜ |
| IMP-4.2 | Implement `generate_data.py` (synthetic data generator) | `benchmarks/generate_data.py` | — | ⬜ |
| IMP-4.3 | Implement `conftest.py` (session fixture, data fixtures) | `benchmarks/conftest.py` | — | ⬜ |
| IMP-4.4 | Implement Benchmark A: Overhead (IrisPark SQL vs hand-written) | `benchmarks/test_overhead.py` | — | ⬜ |
| IMP-4.5 | Implement Benchmark D: Storage (row vs columnar vs indexes) | `benchmarks/test_storage.py` | — | ⬜ |
| IMP-4.6 | Implement Benchmark F: Ingestion (row-by-row vs batch vs LOAD DATA) | `benchmarks/test_ingestion.py` | — | ⬜ |
| IMP-4.7 | Add result logging to `benchmarks/results/` | `benchmarks/conftest.py` | — | ⬜ |

**Verification:**
- `python -m pytest benchmarks/ -v` — all benchmarks pass
- Benchmark A shows <5% overhead
- Benchmark D shows columnar speedup for analytical queries
- Benchmark F shows batch INSERT is significantly faster than row-by-row

---

### 4.6 Phase 5: Governance Artifacts

**Duration:** 1 day
**Dependencies:** None (can run in parallel)
**Deliverables:**
- `SECURITY.md`
- Known limitations documented
- Version consistency verified

**Tasks:**

| ID | Task | File | Owner | Status |
|----|------|------|-------|--------|
| IMP-5.1 | Create `SECURITY.md` with reporting process | `SECURITY.md` (new) | — | ⬜ |
| IMP-5.2 | Document known limitations in README | `README.md` | — | ⬜ |
| IMP-5.3 | Verify version consistency across all files | `pyproject.toml`, `CHANGELOG.md` | — | ⬜ |

**Verification:**
- `SECURITY.md` exists and contains security contact and disclosure policy
- README has a "Known Limitations" section
- `pyproject.toml` version matches `CHANGELOG.md`

---

### 4.7 Implementation Summary

| Phase | Duration | Tasks | Dependencies |
|-------|----------|-------|--------------|
| Phase 0: Version Hygiene | 30 min | 2 | None |
| Phase 1: Bulk Ingestion | 2-3 days | 6 | Phase 0 |
| Phase 2: SQL Generator Tests | 3-4 days | 19 | Phase 0 |
| Phase 3: Columnar Awareness | 2-3 days | 9 | Phase 0 |
| Phase 4: Benchmark Suite | 3-4 days | 7 | Phases 1-3 |
| Phase 5: Governance | 1 day | 3 | None |
| **Total** | **~3 weeks** | **46** | |

**Parallelism:** Phases 1, 2, 3, and 5 can run concurrently. Phase 4 requires Phases 1-3.

---

## 5. Verification Phase

**Entry criteria:** All implementation phases complete. All unit tests passing.

**Exit criteria:** All verification items passed. No P0/P1 bugs open.

### 5.1 Test Plan

#### 5.1.1 Unit Tests

| Suite | Tests | Command | Expected |
|-------|-------|---------|----------|
| SQL Generator | 30+ | `pytest tests/test_sql_generator.py -v` | All pass, no IRIS required |
| Existing offline | 335 | `pytest -m "not online"` | All pass |
| Existing online | 439 | `pytest -m "online"` | All pass |

#### 5.1.2 Integration Tests

| Test | Description | Expected |
|------|-------------|----------|
| `test_batch_insert_performance` | `createDataFrame(pandas_df)` with 1000 rows | <1 second |
| `test_batch_insert_large` | `createDataFrame(pandas_df)` with 100K rows | <2 seconds |
| `test_storage_type_columnar` | `df.write.storageType("columnar").saveAsTable("t")` | Table created with columnar storage |
| `test_columnar_index` | `df.iris.createColumnarIndex("col")` | Index created successfully |
| `test_pretty_sql` | `df.to_sql(pretty=True)` | Returns indented, multi-line SQL |

#### 5.1.3 Benchmark Verification

| Benchmark | Success Criteria |
|-----------|-----------------|
| A: Overhead | IrisPark SQL execution time ≤ 1.05 × hand-written SQL execution time |
| D: Storage | Columnar storage shows measurable speedup for aggregation queries |
| F: Ingestion | Batch INSERT ≥ 10× faster than row-by-row for 100K rows |

#### 5.1.4 Regression Tests

| Check | Command |
|-------|---------|
| All 774 existing tests pass | `python -m pytest tests/ -v` |
| No new mypy errors | `mypy irispark/` |
| No new ruff errors | `ruff check irispark/` |

### 5.2 Acceptance Criteria

| ID | Criterion | Status |
|----|-----------|--------|
| AC-1 | `createDataFrame(pandas_df)` with 100K rows completes in <2s | ⬜ |
| AC-2 | SQL generator unit test suite (30+ tests) passes with no IRIS | ⬜ |
| AC-3 | `to_sql(pretty=True)` produces readable, indented SQL | ⬜ |
| AC-4 | `df.write.storageType("columnar").saveAsTable("t")` works | ⬜ |
| AC-5 | `df.iris.createColumnarIndex("col")` works | ⬜ |
| AC-6 | `df.iris.explain()` returns execution plan | ⬜ |
| AC-7 | Benchmark A: overhead <5% | ⬜ |
| AC-8 | Benchmark D: columnar speedup demonstrated | ⬜ |
| AC-9 | Benchmark F: batch INSERT ≥10× faster | ⬜ |
| AC-10 | Version consistency: pyproject.toml = CHANGELOG | ⬜ |
| AC-11 | `SECURITY.md` exists | ⬜ |
| AC-12 | Known limitations documented | ⬜ |
| AC-13 | All 774 existing tests pass | ⬜ |
| AC-14 | No breaking API changes since 0.9.4 | ⬜ |

### 5.3 Bug Severity Classification

| Severity | Definition | Action |
|----------|-----------|--------|
| P0 — Blocker | Data corruption, SQL injection, crash on basic operations | Must fix before release |
| P1 — Critical | Wrong results, performance regression >50%, broken documented feature | Must fix before release |
| P2 — Major | Missing error handling, edge case failure, cosmetic issue | Fix if time permits |
| P3 — Minor | Typo, missing type hint, non-critical improvement | Defer to post-1.0 |

### 5.4 Verification Sign-off

| Role | Name | Date | Status |
|------|------|------|--------|
| QA Lead | — | — | ⬜ |
| Technical Lead | — | — | ⬜ |

---

## 6. Release Phase

**Entry criteria:** All verification items passed. No P0/P1 bugs open.

**Exit criteria:** v1.0.0 released. Tag created. Package built.

### 6.1 Release Checklist

| ID | Task | Owner | Status |
|----|------|-------|--------|
| REL-1 | Bump version to `1.0.0` in `pyproject.toml` | — | ⬜ |
| REL-2 | Update classifier to `"Development Status :: 5 - Production/Stable"` | — | ⬜ |
| REL-3 | Add `## [1.0.0]` entry to `CHANGELOG.md` summarizing all changes | — | ⬜ |
| REL-4 | Update `README.md` version references and status table | — | ⬜ |
| REL-5 | Run full test suite: `python -m pytest tests/ -v` | — | ⬜ |
| REL-6 | Run lint: `ruff check irispark/` | — | ⬜ |
| REL-7 | Run type check: `mypy irispark/` | — | ⬜ |
| REL-8 | Build package: `python -m build` | — | ⬜ |
| REL-9 | Verify wheel: `pip install dist/irispark-1.0.0-py3-none-any.whl` | — | ⬜ |
| REL-10 | Create git tag: `git tag v1.0.0` | — | ⬜ |
| REL-11 | Create GitHub release with CHANGELOG notes | — | ⬜ |

### 6.2 Release Artifacts

| Artifact | Description |
|----------|-------------|
| `dist/irispark-1.0.0-py3-none-any.whl` | Built wheel |
| `dist/irispark-1.0.0.tar.gz` | Source distribution |
| Git tag `v1.0.0` | Release tag |
| GitHub Release | Release notes + assets |

### 6.3 Rollback Plan

If a P0 issue is discovered after release:
1. Create `v1.0.1` with the fix
2. Do NOT delete or modify the `v1.0.0` tag
3. Document the issue in CHANGELOG under `[1.0.1]`

### 6.4 Release Sign-off

| Role | Name | Date | Status |
|------|------|------|--------|
| Product Owner | — | — | ⬜ |
| Technical Lead | — | — | ⬜ |
| Release Manager | — | — | ⬜ |

---

## 7. Post-1.0 Roadmap

### 7.1 v1.1 — IRIS Diagnostics & Bulk Loading (2-3 weeks)

| ID | Feature | Priority | Effort |
|----|---------|----------|--------|
| P1-1 | `read.load_data(path)` using IRIS `LOAD DATA FROM FILE` | P1 | 2 days |
| P1-2 | `read.jdbc()` using IRIS `LOAD DATA FROM JDBC` | P1 | 2 days |
| P1-3 | `df.iris.tableStats()` — row count, storage type, block usage | P1 | 1 day |
| P1-4 | `df.iris.showIndexes()` — list indexes on table | P1 | 1 day |
| P1-5 | `df.iris.suggestIndexes()` — index recommendations based on filters/joins | P2 | 3 days |
| P1-6 | Benchmark B: Volume (1M, 10M, 100M rows) | P1 | 2 days |
| P1-7 | Benchmark E: Sharding (if cluster available) | P1 | 3 days |

### 7.2 v2.0 — MLflow & Medallion (6-8 weeks)

| ID | Feature | Priority | Effort |
|----|---------|----------|--------|
| P2-1 | MLflow tracking DB on IRIS | P2 | 1 week |
| P2-2 | MLflow artifact store on S3 | P2 | 3 days |
| P2-3 | MLflow model registry on IRIS | P2 | 3 days |
| P2-4 | Medallion automation (`df.write.medallion("gold")`) | P2 | 3 weeks |
| P2-5 | `df.iris.vectorSearch()` — IRIS vector search wrapper | P2 | 1 week |

### 7.3 v3.0 — Advanced Features (10-12 weeks)

| ID | Feature | Priority | Effort |
|----|---------|----------|--------|
| P3-1 | Semantic layer (auto-join discovery via FK metadata) | P3 | 4 weeks |
| P3-2 | CDC / Streaming DataFrame API | P3 | 4 weeks |
| P3-3 | Feature store API with point-in-time correctness | P3 | 4 weeks |

### 7.4 v4.0+ — Scale & Integration (TBD)

| ID | Feature | Priority |
|----|---------|----------|
| P4-1 | Iceberg integration (optional, for 50TB+ historical data) | P4 |
| P4-2 | Multi-source federation (S3 + Oracle + DB2 without prior ingestion) | P4 |
| P4-3 | Spark Connect compatibility | P4 |
| P4-4 | SQL generator incremental refactoring (logical plan IR) | P4 |

---

## 8. Appendix: Architecture Reference

### 8.1 IRIS Capabilities IrisPark Leverages

| IRIS Capability | How IrisPark Uses It | Exposed via API? |
|-----------------|---------------------|------------------|
| SQL Engine + CBO | All DataFrame operations push down to IRIS SQL | Implicit (SQL generation) |
| Columnar storage (`WITH STORAGETYPE = COLUMNAR`) | `df.write.storageType("columnar")` | ✅ v1.0 |
| Columnar indexes (`CREATE COLUMNAR INDEX`) | `df.iris.createColumnarIndex()` | ✅ v1.0 |
| Bitmap indexes (`CREATE BITMAP INDEX`) | `df.iris.createBitmapIndex()` | ✅ v1.0 |
| Sharding (transparent query distribution) | Generated SQL benefits automatically | Implicit (no API needed) |
| ECP (distributed caching) | Generated SQL benefits automatically | Implicit (no API needed) |
| Reporting async mirrors | Connect IrisPark to mirror member | Deployment pattern |
| `LOAD DATA` (bulk ingestion) | `read.load_data()` | ✅ v1.1 |
| `EXPLAIN` (execution plans) | `df.explain()`, `df.iris.explain()` | ✅ v1.0 |
| ObjectScript UDFs (16 built-in) | Auto-installed on session init | Implicit |
| `%EXACT` collation | String comparisons in Column API | Implicit |
| `%ID` pseudo-column | LCG-based sampling, dedup tiebreaker | Implicit |

### 8.2 IRIS Capabilities Not Yet Leveraged

| IRIS Capability | Potential API | Target Version |
|-----------------|---------------|----------------|
| Vector search | `df.iris.vectorSearch()` | v2.0 |
| Table statistics (`%SYS.PTools`) | `df.iris.tableStats()` (enhanced) | v1.1 |
| Index recommendations | `df.iris.suggestIndexes()` | v1.1 |
| Work Queue Manager (parallel processing) | N/A (IRIS handles parallelism) | N/A |
| Embedded Python (`LANGUAGE PYTHON`) | UDF registration (already partially supported) | v2.0 |
| Auditing | `df.iris.auditInfo()` | v3.0 |
| CDC / Interoperability | `spark.readStream()` | v3.0 |

### 8.3 Enterprise Deployment Pattern

```
                Operational Applications
                         │
                         ▼
                    IRIS Primary
                         │
                      Mirroring
                         │
                         ▼
              Reporting Async Member
                         │
                 ┌───────┴───────┐
                 │               │
                 ▼               ▼
              IrisPark        BI / SQL
                 │
          ┌──────┴──────┐
          │             │
          ▼             ▼
       Jupyter      Python ML
                     Ecosystem
```

For larger analytical workloads with sharding:

```
                   IrisPark
                      │
                      ▼
                IRIS SQL Layer
                      │
                      ▼
             IRIS Sharded Cluster
             ┌────────┼────────┐
             │        │        │
             ▼        ▼        ▼
         Shard A  Shard B  Shard C
```

### 8.4 Key IRIS Documentation References

| Topic | Document |
|-------|----------|
| Columnar storage | [Choose an SQL Table Storage Layout](https://docs.intersystems.com/irislatest/csp/docbook/DocBook.UI.Page.cls?KEY=GSOD_storage) |
| Sharding | [Scale for Data Volume with Sharding](https://docs.intersystems.com/irislatest/csp/docbook/DocBook.UI.Page.cls?KEY=GSCALE_sharding) |
| Bulk loading | [LOAD DATA](https://docs.intersystems.com/irislatest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_loaddata) |
| CREATE TABLE | [CREATE TABLE](https://docs.intersystems.com/irislatest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_createtable) |
| Columnar indexes | [CREATE INDEX](https://docs.intersystems.com/irislatest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_createindex) |
| Mirroring | [High Availability Guide](https://docs.intersystems.com/irislatest/csp/docbook/DocBook.UI.Page.cls?KEY=GHA_mirror) |

---

## Document Control

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-08-11 | — | Initial waterfall plan for v1.0.0 |
