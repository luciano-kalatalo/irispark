# IrisPark

**PySpark-like analytics engine for InterSystems IRIS.**

Write PySpark-fluent DataFrame code. Execute everything natively on IRIS via SQL pushdown. No Spark cluster required.

IrisPark is a Python-based analytics and machine learning framework that provides a Databricks-equivalent developer experience entirely over InterSystems IRIS.



## Vision

> **Analyze the data where it already lives, using the experience you already know.**

IrisPark is **not** "Apache Spark implemented on IRIS". It's a **DataFrame-native analytics layer** that makes IRIS capabilities reachable through a familiar API:

- **Preserves a familiar mental model** — PySpark syntax data engineers already know
- **Hides infrastructure complexity** — no need to learn ObjectScript or IRIS internals
- **Keeps execution close to the data** — all operations push down to IRIS SQL
- **Delivers an excellent developer experience** — lazy evaluation, Arrow bridge, clear error messages
- **Exposes IRIS superpowers** — vector search, storage info, index recommendations (diagnostics shipped in v1.2; vector search planned v2.0)

## Installation

Requires **Python 3.10+**.

### From wheel (recommended)

```bash
pip install irispark-1.6.0-py3-none-any.whl
```

### From local path (dev)

```bash
git clone <repo-url>
cd irispark
pip install -e .

or

docker compose up --build

```

### Optional extras

| Use case | Install command |
|---|---|
| Dask DataFrame support | `pip install -e ".[dask]"` |
| Polars DataFrame support | `pip install -e ".[polars]"` |
| JDBC foreign tables | `pip install -e ".[jdbc]"` |
| Jupyter notebook kernel | `pip install -e ".[jupyter]"` |
| Everything | `pip install -e ".[all]"` |

## Quick Start

```python
from irispark import IrisParkSession

iris = IrisParkSession.builder() \
    .host("localhost") \
    .port(3972) \
    .namespace("DATASPARK") \
    .username("suser") \
    .password("pass123") \
    .getOrCreate()

# Query a table
df = iris.table("sales")

# Lazy chain — no SQL executed yet
result = (
    df.filter("year = 2025")
      .group_by("region")
      .agg({"amount": "sum"})
      .order_by("sum_amount DESC")
      .limit(10)
)

# Trigger execution
result.show()
result.to_pandas()
```

## Core Principles

1. **DataFrame-first experience** — Fluent PySpark-like API
2. **IRIS-first execution** — All operations push down to IRIS SQL
3. **Lazy evaluation** — Transformations build a DAG, actions trigger execution
4. **SQL transparency** — `df.to_sql()` shows generated SQL, `df.explain()` shows IRIS plan
5. **Simplicity over feature parity** — Focus on common analytics patterns
6. **Native IRIS extensions** — Expose IRIS capabilities through DataFrame API

### Using IrisSparkSession (SCOPE-compliant alias)

```python
from irispark import IrisSparkSession

spark = IrisSparkSession.builder \
    .appName("Sales Forecast") \
    .config("iris.host", "localhost") \
    .config("iris.namespace", "USER") \
    .getOrCreate()
```

### Using sparkContext (RDD API)

```python
sc = iris.sparkContext
rdd = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.map(lambda x: x * 2).filter(lambda x: x > 5).collect()
# → [6, 8, 10]

# Bridge to DataFrame
df = rdd.toDF(["value"])
```

### Window Functions

```python
from irispark.functions import row_number, rank, lag, lead, col
from irispark import Window

w = Window.partitionBy("department").orderBy(col("salary").desc())

df.withColumn("rn", row_number().over(w)) \
  .withColumn("prev_salary", lag(col("salary"), 1).over(w)) \
  .show()
```

### Read / Write

```python
# Read from IRIS tables
df = iris.read.table("sales")

# Read from files
df = iris.read.parquet("s3://bucket/data.parquet")
df = iris.read.csv("data.csv")

# Read local or S3 files through IRIS Foreign Tables
# IRIS owns the file connection; rows are not copied into Python.
df = iris.read.parquet("s3://bucket/data.parquet", foreign=True)
df = iris.read.csv("data.csv", foreign=True, options={"header": True})

# Read from remote JDBC sources via IRIS Foreign Tables
# No data is copied locally; IRIS queries the remote table directly.
df = iris.read.jdbc(
    url="jdbc:postgresql://dbserver/sourcedb",
    dbtable="public.accounts",
    user="analyst",
    password="...",
    driver="org.postgresql.Driver",
)

# Write back to IRIS
df.write.saveAsTable("gold_features")

# Write back to a remote JDBC table via IRIS Foreign Table
# IRIS evaluates the DataFrame SQL and pushes rows through the foreign server.
df.write.jdbc(
    url="jdbc:postgresql://dbserver/sourcedb",
    dbtable="public.scores",
    user="analyst",
    password="...",
    driver="org.postgresql.Driver",
    mode="overwrite",
)

# Export
df.write.parquet("output.parquet")
df.write.csv("output.csv")
```

### Pandas / Polars interop

```python
import pandas as pd
import polars as pl

# From DataFrame backends
df = iris.createDataFrame(pandas_df)
df = iris.createDataFrame(polars_df)

# To DataFrame backends
pdf = df.to_pandas()
plf = df.to_polars()
```

## Features

### DataFrame API
- **Lazy evaluation** — transformations build a DAG, only actions trigger SQL execution
- **SQL pushdown** — all filtering, aggregation, grouping, sorting execute inside IRIS
- **80+ SQL functions** — math, string, date, conditional, aggregate, hash, window
- **Window functions** — `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `ntile`, and more

- **`summary()`** — PySpark-compatible summary statistics. Returns count, mean, stddev, min, 25%, 50%, 75%, max (default). Works on both numeric and string columns. Supports custom statistics: `df.summary("count", "min", "max")`.
- **JOIN support** — inner, left, right, full outer, left semi, left anti, cross join
- **Advanced operations** — pivot, unpivot, CUBE, ROLLUP, sample, randomSplit

### RDD API
- **In-memory RDD** — `parallelize`, `map`, `filter`, `flatMap`, `reduce`, `collect`
- **Bridge to DataFrame** — `rdd.toDF()` converts to IrisDataFrame

### ML (`irispark.ml`)
A PySpark `pyspark.ml`-compatible framework. See the [API Reference](docs/api_reference.md) and `ml_scope.md`.

- **Core framework** — `Transformer`/`Estimator`/`Model`, `Pipeline`/`PipelineModel`, `Param`/`Params`, `LogicalVector`, `MLSemanticPlanner`
- **Feature transformers** — `VectorAssembler`, `StringIndexer`, `OneHotEncoder`, `StandardScaler`, `QuantileDiscretizer`, `Imputer`, `Binarizer`, `MinMaxScaler`, `MaxAbsScaler`, `IndexToString`, `SQLTransformer`
- **Supervised estimators** — `LinearRegression`, `LogisticRegression` (numpy fit, SQL-pushdown inference)
- **Ensemble (EPython/sklearn)** — `RandomForestClassifier/Regressor`, `KNeighborsClassifier/Regressor`
- **Evaluation** — `RegressionEvaluator` (MAE/MSE/RMSE/R²), `BinaryClassificationEvaluator` (accuracy/precision/recall/F1/AUC)
- **Tuning** — `ParamGridBuilder`, `CrossValidator`, `TrainValidationSplit`
- **Persistence** — `save`/`load`/`load_by_name`/`list_models`/`delete_model`
- **AutoML (IRIS extension)** — `AutoMLClassifier`/`AutoMLRegressor`/`AutoMLModel`, `CustomModelClassifier`

### Interoperability
- **Pandas / Polars / Dask** — auto-detected by `createDataFrame`
- **Zero-copy Arrow** — results materialize via Apache Arrow
- **Read/Write** — parquet, csv, jdbc, saveAsTable, insertInto

### Foreign Tables (DS 0.1 / DS 0.2 / DS 0.3)
- `read.jdbc()` registers a transient IRIS Foreign Server + Foreign Table pointing at a remote JDBC table
- `read.parquet(path, foreign=True)` / `read.csv(path, foreign=True)` register a file-backed IRIS Foreign Server + Foreign Table for local or S3 paths
- `df.write.jdbc()` writes back through an IRIS Foreign Table (`INSERT INTO ... SELECT ...`)
- `df.write.saveAsForeignTable()` publishes to an existing foreign server
- No local data copy for reads or writes; IRIS moves rows through the foreign server
- Cross-source joins: an IRIS table and a foreign table join in a single pushed-down SQL query
- Session lifecycle: transient foreign tables/servers created by `read.jdbc()` / file reads are dropped on `session.close()`
- Persistent foreign tables opt-in with `persistent=True` for shared Bronze/Silver views
- `session.iris` namespace: `register_jdbc_foreign_table()`, `register_file_foreign_table()`, `create_foreign_table_from_query()`, `drop_foreign_table()`, `foreign_tables()`
- `df.iris.foreign` namespace: `is_foreign_table()`, `server_name()`, `is_persistent()`, `refresh()`

### Diagnostics
- **Explain plans** — `df.explain()` shows logical plan + IRIS explain plan
- **Lineage tracking** — `df.lineage(show=True)` shows transformation history
- **SQL transparency** — `df.to_sql()` shows generated SQL
- **IRIS session extensions** — `session.iris` for foreign-table lifecycle
- **Foreign table introspection** — `df.iris.foreign` for server name and cleanup helpers

## Dependencies

| Dependency | Required | Notes |
|-----------|----------|-------|
| `intersystems-irispython` | ✅ | IRIS Python DB-API driver (from PyPI) |
| `pandas` | ✅ | Default DataFrame backend |
| `pyarrow` | ✅ | Zero-copy exchange format |
| `python-dotenv` | ✅ | Environment variable loading |
| `polars` | Optional | Polars DataFrame backend (`df.to_polars()`) |
| `dask[dataframe]` | Optional | Extra DataFrame backend |
| `sqlalchemy` | Optional | Required for `read.jdbc()` |
| `jupyter`, `ipykernel` | Optional | Jupyter notebook kernel support |

## Running Tests

```bash
# Offline tests (no IRIS needed)
python -m pytest tests/test_rdd.py

# All tests
python -m pytest tests/
```

## Current Status (v1.6.0)

| Component | Status | Tests |
|-----------|--------|-------|
| DataFrame API | ✅ Production-ready | 1,200+ total test functions |
| SQL pushdown | ✅ All operations | 660+ offline / 570+ online-marked |
| Lazy evaluation | ✅ DAG-based | |
| Window functions | ✅ 11 functions | |
| RDD API | ✅ In-memory | |
| ML feature transformers | ✅ 11 transformers | `VectorAssembler`, `StringIndexer`, `OneHotEncoder`, `StandardScaler`, `QuantileDiscretizer`, `Imputer`, `Binarizer`, `MinMaxScaler`, `MaxAbsScaler`, `IndexToString`, `SQLTransformer` |
| ML supervised & tuning | ✅ estimators + evaluation + tuning + persistence | `LinearRegression`, `LogisticRegression`, RF/KNN, evaluators, CV/TVS, model save/load |
| Read/Write | ✅ parquet, csv, json, jdbc | |
| Diagnostics | ✅ explain, lineage, show_stats, show_indexes, suggest_indexes | |
| Column API | ✅ PySpark parity (Tier-1: 12 methods) | `isNaN`, `eqNullSafe`, `ilike`, `when/otherwise`, `substr`, `astype`, `name`, `asc/desc_nulls_first/last` |
| GroupedData | ✅ PySpark parity (Tier-1) | `mean`, `pivot(values=None)` auto-distinct, `agg` guard |
| pandas I/O functions | ✅ PySpark parity (Tier-1) | `read_csv`, `read_parquet`, `read_json`, `read_table`, `read_sql`, `read_sql_query`, `read_sql_table`, `from_pandas` |
| Test coverage | ✅ 660+ pass in CI (`pytest tests/`; online-marked tests need IRIS) | `pytest tests/ -m ""` runs everything |

## Roadmap

### v1.0 (Complete) — Core Platform
- ✅ DataFrame API with PySpark syntax
- ✅ SQL pushdown to IRIS
- ✅ Lazy evaluation
- ✅ Window functions
- ✅ ML transformers
- ✅ RDD API
- ✅ Read/Write (parquet, csv, jdbc)

### v1.2 Complete — IRIS Diagnostics
- [x] `df.iris.show_stats()` — row count, column count, storage size when the server exposes the API
- [x] `df.iris.show_indexes()` — list indexes on table
- [x] `df.iris.suggest_indexes()` — index recommendations from query lineage + cardinality

### v1.3 Complete — PySpark Column API Parity (Tier-1)
- [x] Null-sensitive sort helpers (`asc/desc_nulls_first/last`) via `CASE WHEN ... IS NULL` emulation
- [x] `ilike`, `eqNullSafe`, `isNaN`, `when/otherwise`, `substr`, `astype`, `name`

### v1.4 Complete — PySpark GroupedData Parity (Tier-1)
- [x] `GroupedData.mean()` — alias of `avg()` (`AVG` on IRIS)
- [x] `GroupedData.pivot(values=None)` — eager distinct scan of the pivot column, then CASE-WHEN pivot emulation
- [x] `GroupedData.agg()` empty guard (PySpark contract)

### v1.5 Complete — PySpark pandas I/O Parity (Tier-1)
- [x] Module-level `read_csv`, `read_parquet`, `read_json`, `read_table` (format inference), `read_sql`, `read_sql_query`, `read_sql_table`, `from_pandas` in `irispark.io`
- [x] `Read.json` — new public reader (local NDJSON via pyarrow + foreign JSON wrapper)

### v2.0 — Native IRIS Extensions
- [ ] Vector search wrapper (`df.iris.vector_search()` — IRIS 2026 exposes `EMBEDDING`, `TO_VECTOR`, `VECTOR_COSINE`, `VECTOR_DOT_PRODUCT`)
- [ ] Medallion automation (Bronze/Silver/Gold)
- [ ] MLflow integration (tracking DB on IRIS, artifacts on S3)
- [ ] IRIS SQL function coverage gap-fill (91 of the IRIS 2026 SQL function catalog are not yet wrapped) — e.g. `DATE_TRUNC`, `CHARINDEX`, `STUFF`, `ACOS/ASIN/ATAN/ATAN2`
- [ ] SQL generation cleanup (reduce subquery nesting)
- [ ] JSON SQL (`from_json`, `to_json`, `get_json_object`)
- [ ] Vectorized / pandas UDFs via Embedded Python
- [ ] DecisionTree / GBT classifiers and regressors
- [ ] KMeans, PCA

### v3.0 — Advanced Features
- [ ] Semantic layer (auto-join discovery via FK metadata)
- [ ] CDC / Streaming DataFrame API
- [ ] Feature store API with point-in-time correctness
- [ ] Semi-structured flatten (`%DynamicObject` / JSON path)
- [ ] Bulk stage / COPY-style file ingest

## Known Limitations

| Limitation | Status | Notes |
|------------|--------|-------|
| Single-node only | By design | IRIS Sharding handles distribution at SQL layer |
| No streaming/CDC | Planned v3.0 | Batch-oriented; use IRIS Interoperability for real-time |
| No Delta Lake / Iceberg | By design | IRIS tables are the storage layer |
| `read_delta` / `read_orc` / `read_excel` / `read_html` / `read_pickle` | By design | No IRIS backing; use `df.write.*` / `df.to_pandas()` |
| `groupingSets` | Blocked | Unsupported on IRIS 2026.2; re-check on future IRIS releases |
| No Spark Connect | By design | Not needed for target use case |
| IRIS-only | By design | Lock-in is the value proposition |
| Immutability convention-only | Operational | Use IRIS audit logging + S3 Object Lock for compliance |
| No MLflow integration yet | Planned v2.0 | Tracking DB on IRIS, artifacts on S3 |
| `LOAD DATA` for ingestion | Partial | Works for server-side files; not for in-memory DataFrames |
| JDBC foreign tables | Session-scoped | Foreign servers/tables created by `read.jdbc()` are dropped on `session.close()` |
| Storage size in `show_stats()` | Partial | `%SYS.GlobalQuery*` SQL API removed in IRIS 2026.x; storage keys omitted on such servers |

## Architecture

```
User Code (PySpark-like API)
         ↓
IrisPark API Layer (Session, DataFrame, Column, Functions)
         ↓
Lazy SQL Generator (SQLGenerator — builds DAG, emits IRIS SQL)
         ↓
InterSystems IRIS SQL Engine (optimizer, parallel execution, indexes)
         ↓
Arrow RecordBatch (client-side bridge)
         ↓
Pandas / Polars / Dask (interop)
```

**Key insight:** IrisPark does NOT build a query optimizer. IRIS already has a world-class cost-based optimizer with predicate pushdown, join reordering, and index selection. Our job is to generate clean SQL and expose IRIS capabilities through a familiar API.

## Documentation

- **[Getting Started](docs/getting_started.md)** — install, first query, verify.
- **[Compatibility Matrix](docs/compatibility.md)** — every SQL function with its PySpark mapping, execution engine, and compatibility level (A–E).
- **[Migration Guide](docs/migration.md)** — classify your PySpark code by adaptation effort (works unchanged → unsupported).
- **[Known Differences](docs/known_differences.md)** — where IrisPark deliberately differs from PySpark.
- **[API Reference](docs/api_reference.md)** — the public API surface.
- **[Architecture Guide](docs/architecture_guide.md)** — layered design and execution model.
- **[Performance Guide](docs/performance_guide.md)** — benchmarks and methodology.
- **[Data Access Guide](docs/data_access.md)** — certified PG/Oracle/TLS/S3 patterns.
- **[Foreign Tables Guide](docs/foreign_tables_guide.md)** — DS 0.1/0.2/0.3 federation.
- **[Security Guide](docs/security_guide.md)** — security model and deployment.
- **[Security Review](docs/security_review.md)** — formal security review artifact (§75 gate).
- **[Deployment Guide](docs/deployment_guide.md)** — packaging, containers, EPython contract.
- **[Benchmark Baseline](docs/benchmark_baseline.md)** — committed performance baseline (§74).
- **[Upgrade Guide](docs/upgrade_guide.md)** — upgrading IrisPark and the IRIS engine.
- **[Troubleshooting](docs/troubleshooting.md)** — common issues and fixes.
- **[Release Notes](docs/release_notes.md)** — release history summary.
- **Diagnostics** — `df.explain(extended=True)` prints the logical plan, lineage, and registered function capabilities.

## Notebooks

A progressive Jupyter notebook series that tests IrisPark against a live IRIS instance. Each notebook is self-contained, connects via environment variables, and skips gracefully when IRIS is unreachable.

```bash
pip install -e ".[jupyter]"
jupyter notebook notebooks/
```

| Notebook | Covers |
|---|---|
| [00 — Setup & Connect](notebooks/00_setup_and_connect.ipynb) | Environment, connect, `irispark-doctor`, verify `vendas` |
| [01 — DataFrame Basics](notebooks/01_dataframe_basics.ipynb) | `select`, `filter`, `withColumn`, `orderBy`, actions, schema, `describe` |
| [02 — Functions](notebooks/02_functions.ipynb) | Math/string/date/conditional/hash functions + IRISPARK-native aggregates |
| [03 — Grouping & Aggregation](notebooks/03_grouping_aggregation.ipynb) | `groupBy`, `agg`, `pivot`, `cube`/`rollup`, `df.stat` |
| [04 — Joins & Unions](notebooks/04_joins_unions.ipynb) | Join types, `union`/`unionByName`, `distinct`, `alias` |
| [05 — Window Functions](notebooks/05_window_functions.ipynb) | `row_number`, `rank`, `lag`/`lead`, frames |
| [06 — Null Handling](notebooks/06_null_handling.ipynb) | `na` namespace, `dropna`/`fillna`, null-aware functions |
| [07 — Read / Write](notebooks/07_read_write.ipynb) | CSV/Parquet/JSON, `saveAsTable`, `createDataFrame`, `irispark.io` |
| [08 — ML Transformers](notebooks/08_ml_transformers.ipynb) | `VectorAssembler`, `StringIndexer`, `OneHotEncoder`, `StandardScaler`, `QuantileDiscretizer` |
| [09 — RDD & Observability](notebooks/09_rdd_observability.ipynb) | RDD API, `explain`, `lineage`, `to_sql`, session metrics |
| [10 — Known Differences](notebooks/10_known_differences.ipynb) | Documented deviations vs PySpark |

### Data Scientist series

| Notebook | Focus |
|---|---|
| [11 — Exploratory Data Analysis](notebooks/11_ds_exploratory_data_analysis.ipynb) | Profiling, missing-data map, distributions, outliers, correlations, crosstabs, pandas handoff |
| [12 — Feature Engineering Pipeline](notebooks/12_ds_feature_engineering.ipynb) | Cleaning, date parts, bucketing, window features, ML transformer chain, materialization |
| [13 — Sampling, Splits & Segments](notebooks/13_ds_sampling_segments.ipynb) | Reproducible sampling, train/test split, stratified draws, deciles, cohort pivots |

### Data Engineer series

| Notebook | Focus |
|---|---|
| [14 — Ingestion & Bulk Load](notebooks/14_de_ingestion_bulk_load.ipynb) | Chunked multi-row inserts at 10k rows, type inference, LOAD DATA, foreign reads |
| [15 — Table Lifecycle & Storage](notebooks/15_de_table_lifecycle_storage.ipynb) | Writer modes, columnar storage, temp views, catalog inspection, cleanup |
| [16 — Data Quality Checks](notebooks/16_de_data_quality_checks.ipynb) | Completeness, uniqueness, referential integrity (`left_anti`), validity, quarantine |
| [17 — Operations & Observability](notebooks/17_de_operations_observability.ipynb) | Cache/persist, execution plans, lineage, per-query metrics, UDF registration |

### Extension series

| Notebook | Focus |
|---|---|
| [18 — Time-Series Analytics](notebooks/18_ds_time_series_analytics.ipynb) | Daily rollups, moving averages, month-over-month deltas, gap detection, sessionization |
| [19 — Federation via Foreign Tables](notebooks/19_de_federation_foreign_tables.ipynb) | JDBC foreign tables, cross-source joins, `writer.jdbc` write-back, file federation |
| [20 — UDF Authoring & Registry](notebooks/20_de_udf_authoring.ipynb) | Three execution tiers (native/ObjectScript/EPython), custom UDFs, registry inventory |
| [21 — PySpark Migration Guide](notebooks/21_pyspark_migration_guide.ipynb) | Side-by-side idiom mapping, parity checklist, migration gotchas |
| [22 — Supervised ML](notebooks/22_ds_supervised_ml.ipynb) | Estimators, evaluators, AutoML, tuning — the full §73 workflow |
| [23 — LogicalVector & Planner](notebooks/23_ml_vector_planner.ipynb) | `featuresCol` as `LogicalVector`, planner backend metadata, pipeline vectors/backends |

### ML Analyst series

| Notebook | Focus |
|---|---|
| [24 — Prepare & Split](notebooks/24_mla_prepare_and_split.ipynb) | Credit dataset, feature prep, leak-free train/test split, stratification |
| [25 — Regression Workflow](notebooks/25_mla_regression_workflow.ipynb) | `LinearRegression` fit → SQL predict → MAE/MSE/RMSE/R², baseline comparison |
| [26 — Classification Workflow](notebooks/26_mla_classification_workflow.ipynb) | `LogisticRegression`, probability/prediction, accuracy/precision/recall/F1/AUC, threshold sensitivity |
| [27 — Tune, Select & Score](notebooks/27_mla_tune_and_score.ipynb) | `CrossValidator` vs `TrainValidationSplit`, best-model selection, batch scoring, persist scored table |
| [28 — Model Persistence](notebooks/28_mla_model_persistence.ipynb) | Save/load/list/delete models, reload by name, persist pipelines, metadata round-trip |
| [29 — Estimator Bridge](notebooks/29_mla_estimator_bridge.ipynb) | Trees/KNN via EPython sklearn backend, AutoML custom models, CatBoost/TensorFlow drop-ins |
| [30 — CatBoost Full Circle](notebooks/30_mla_catboost_full_circle.ipynb) | CatBoost trained server-side via Embedded Python, predict/evaluate/score new applicants |
| [31 — PySpark ML Example](notebooks/31_pyspark_ml_example.ipynb) | Classic salary regression walkthrough: `VectorAssembler` → `LinearRegression` fit/predict/evaluate |

### Production Workflow series

| Notebook | Focus |
|---|---|
| [32 — Landing & Ingest](notebooks/32_pw_ingest.ipynb) | CSV/Parquet/foreign-table read → landing table |
| [33 — Clean & Transform](notebooks/33_pw_transform.ipynb) | Data quality, date parts, window features → Silver table |
| [34 — Train & Serve](notebooks/34_pw_train_serve.ipynb) | Fit → evaluate → persist → batch-score new rows |
| [35 — Federate & Govern](notebooks/35_pw_federate_govern.ipynb) | Foreign table, cross-source join, write-back, credential hygiene |
| [36 — Observe & Optimize](notebooks/36_pw_observe_optimize.ipynb) | `explain`/lineage, metrics, cache, SQL transparency |

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for release history and breaking changes.
