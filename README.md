# IrisPark 

IrisPark is a Python-based analytics and machine learning framework that provides a Databricks-equivalent developer experience entirely over InterSystems IRIS.

**IrisPark is NOT "Apache Spark implemented on IRIS".** It's a **DataFrame-native analytics layer** that:

- Preserves a familiar mental model (PySpark syntax)
- Hides infrastructure complexity
- Keeps execution close to the data (IRIS SQL pushdown)
- Delivers an excellent developer experience
- Exposes IRIS capabilities through DataFrame API



## Table of Contents
- [What is IrisPark?](#what-is-irispark)
- [Why it matters](#why-it-matters)
- [High‑level architecture](#high-level-architecture)
- [Quick example](#quick-example)
- [Requirements](#requirements)
- [Run](#run)
- [Try it](#try-it)
- [What you get](#what-you-get)
- [Connection reference](#connection-reference)
- [Stop](#stop)

## What is ?

IrisPark is a **PySpark‑compatible analytics engine** that runs *natively* on **InterSystems IRIS**.  
It provides the familiar PySpark DataFrame and RDD APIs while translating every operation into **pure IRIS SQL** that executes inside the IRIS engine.

- No Spark cluster, no JVM, no scheduler.
- No data copying – queries run where the data lives.
- Full SQL push‑down; Python is only used for functions that lack a native IRIS implementation.

## Why it matters

| ✅ Benefit | Explanation |
|-----------|-------------|
| **Familiar API** | If you already know PySpark, you can start using IrisPark immediately. |
| **Data‑local execution** | No ETL pipelines, no duplicated data – results are always up‑to‑date. |
| **Leverages IRIS’s engine** | IRIS provides a cost‑based optimizer, indexes, columnar storage and parallel execution. IrisPark simply feeds it clean SQL. |
| **Lightweight runtime** | Only Docker containers (IRIS + Jupyter) are required – no separate Spark installation. |
| **Extensible** | Falls back gracefully to ObjectScript UDFs or Embedded Python when needed. |


## High‑level architecture


flowchart TB
    A[User code (PySpark‑like)] --> B[IrisPark API (Session, DataFrame, Functions)]
    B --> C[Lazy SQL Generator (DAG → IRIS SQL)]
    C --> D[InterSystems IRIS SQL Engine (optimizer, indexes, columnar storage)]
    D --> E[Arrow RecordBatch ↔ Pandas / Polars / Dask]

*Data flow: user code → IrisPark API → lazy DAG → IRIS‑native SQL → Arrow bridge → Python data‑science libraries.*

## Quick example

```python
from irispark import IrisParkSession

iris = IrisParkSession.builder() \
    .host("localhost") \
    .port(3972) \
    .namespace("DATASPARK") \
    .username("suser") \
    .password("pass123") \
    .getOrCreate()

df = iris.table("sales")

result = (
    df.filter("year = 2025")
      .group_by("region")
      .agg({"amount": "sum"})
      .order_by("sum_amount DESC")
      .limit(10)
)

result.show()
pdf = result.to_pandas()
```
All operations are lazily built; SQL is sent to IRIS only when `show()` or `to_pandas()` is called.

## Requirements
- Docker with Docker Compose v2

## Run

```bash
git clone <repo-url>
cd irispark
docker compose up --build
```

Wait for the `iris` container to finish seeding (first boot installs the server‑side ML stack). Then open:

- **Jupyter**: http://localhost:8890  (no token)  
- **IRIS Management Portal**: http://localhost:52774/csp/sys/UtilHome.csp

## Try it

In Jupyter, open `notebooks/01_dataframe_basics.ipynb` and run the cells. The notebooks connect automatically to the compose IRIS (`DATASPARK` namespace, user `suser` / `pass123`).

## What you get
- **IRIS 2026.2** Community, namespace `DATASPARK`, seeded `vendas` table  
- **IrisPark** installed (editable) inside the Jupyter container  
- **32 notebooks** covering the DataFrame API, SQL functions, joins/windows, ML transformers, supervised ML, and more  
- Server‑side **Embedded Python** ML stack (numpy, scikit‑learn, CatBoost, InterSystems AutoML) pre‑provisioned on first boot

## Connection reference

| Service | Host | Port |
|---------|------|------|
| IRIS superserver (SQL) | localhost | 3972 |
| IRIS Management Portal | localhost | 52774 |
| Jupyter | localhost | 8890 |

Inside the Jupyter container, notebooks connect to `iris:1972`.

## Stop

```bash
docker compose down
```
