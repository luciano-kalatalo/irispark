# Getting Started & Installation

This guide gets you from zero to a running IrisPark query against InterSystems IRIS.

---

## 1. Prerequisites

- **Python 3.10+** (CI tests 3.10/3.11/3.12)
- **InterSystems IRIS** 2025.3+ (CI and dev are pinned to **2026.2**)
- **Docker** (optional — for the local IRIS container used in development)

---

## 2. Install IrisPark

### From wheel (recommended for production)

```bash
pip install irispark
```

### From local path (development)

```bash
git clone <repo-url>
cd irispark-mvp
pip install -e .
```

### Optional extras

| Use case | Install command |
|---|---|
| Dask DataFrame support | `pip install -e ".[dask]"` |
| Polars DataFrame support | `pip install -e ".[polars]"` |
| JDBC foreign tables | `pip install -e ".[jdbc]"` |
| Jupyter notebook kernel | `pip install -e ".[jupyter]"` |
| Running the test suite | `pip install -e ".[test]"` |
| Development (tests + lint + type check) | `pip install -e ".[dev]"` |
| Everything | `pip install -e ".[all]"` |

---

## 3. Start IRIS (development)

The project ships a `Makefile` that starts a pinned IRIS Community container and seeds
test data:

```bash
make setup        # start IRIS container + initialize test data
make test-online  # run the full test suite against the container
make clean        # stop and remove the container
```

`make setup` runs `intersystemsdc/iris-community:2026.2`, waits for readiness, and seeds
the `dataspark` namespace with a `vendas` table.

---

## 4. Your first query

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

### Using environment variables (recommended)

```python
import os
from dotenv import load_dotenv
from irispark import IrisParkSession

load_dotenv()

with (
    IrisParkSession.builder()
    .host(os.environ.get("IRIS_HOST", "localhost"))
    .port(int(os.environ.get("IRIS_PORT", 3972)))
    .namespace(os.environ.get("IRIS_NAMESPACE", "DATASPARK"))
    .username(os.environ.get("IRIS_USERNAME", "suser"))
    .password(os.environ.get("IRIS_PASSWORD", "pass123"))
    .getOrCreate()
) as iris:
    df = iris.table("vendas")
    print(df.select("cidade", "valor").filter("estado = 'SP'").to_pandas())
```

See `examples/basic_usage.py` for a runnable version.

### Using the SCOPE-compliant alias

```python
from irispark import IrisSparkSession

spark = IrisSparkSession.builder \
    .appName("Sales Forecast") \
    .config("iris.host", "localhost") \
    .config("iris.namespace", "USER") \
    .getOrCreate()
```

---

## 5. Verify your installation

Run the deployment diagnostic:

```bash
irispark-doctor
```

It checks the IRIS connection and version, Python version, IrisPark version, platform
architecture, CPU flags (AVX/AVX2/BMI/BMI2), and columnar/vector support, then reports
`READY` or `CHECK FAILED`.

---

## 6. Run the test suite

```bash
# Offline tests (no IRIS needed)
python -m pytest tests/test_rdd.py

# All tests (requires a running IRIS)
python -m pytest tests/
```

The default pytest config runs offline tests only (`-m 'not online'`). Use `-m ""` to run
the full suite including online tests against a live IRIS.

---

## 7. Next steps

- **Compatibility Matrix** — [`compatibility.md`](compatibility.md): every SQL function with
  its PySpark mapping, execution engine, and compatibility level (A–E).
- **Migration Guide** — [`migration.md`](migration.md): classify your PySpark code by
  adaptation effort.
- **Known Differences** — [`known_differences.md`](known_differences.md): where IrisPark
  deliberately differs from PySpark.
- **Data Access** — [`data_access.md`](data_access.md): certified PG/Oracle/TLS/S3 patterns.
- **Diagnostics** — `df.explain(extended=True)` prints the logical plan, lineage, and
  registered function capabilities.
