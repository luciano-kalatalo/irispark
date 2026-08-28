# Deployment Guide

This guide covers deploying IrisPark in production: packaging, IRIS-side installation,
containerization, and the deployment contract for Embedded Python UDFs.

---

## 1. Distribution Model

IrisPark is distributed as a Python package (`irispark`) plus an IRIS-side component for
UDFs/UDAFs.

### Python package

```bash
pip install irispark
```

Build a wheel for offline/air-gapped installs:

```bash
pip install build
python -m build
# dist/irispark-<version>-py3-none-any.whl
```

### IRIS-side installation

IrisPark installs its UDFs/UDAFs automatically on the first `IrisParkSession` creation
(`sql.udf.install_all()` and `sql.udaf.install_all()`). No manual IRIS-side package
installation is required for the core engine.

---

## 2. Requirements

- **Python 3.10+**
- **InterSystems IRIS** 2025.3+ (CI and dev pinned to **2026.2**)
- Core deps: `intersystems-irispython`, `pandas`, `pyarrow`, `python-dotenv`
- Optional: `polars`, `dask[dataframe]`, `sqlalchemy` (JDBC), `jupyter`/`ipykernel`

---

## 3. Containerization

### Generic Python container

The repo ships a `Dockerfile` (Python 3.12-slim) that installs the core deps and copies
the workspace:

```dockerfile
FROM python:3.12-slim
WORKDIR /workspace
RUN pip install --no-cache-dir \
    intersystems-irispython pandas polars pyarrow python-dotenv
COPY . .
CMD ["python"]
```

### IRIS container

The dev/CI recipe starts a pinned IRIS Community container with a merge CPF and init
script:

```bash
docker run -d --name iris \
  -p 1972:1972 \
  -e ISC_CPF_MERGE_FILE=/config/merge.cpf \
  -v "$PWD/scripts/merge.cpf:/config/merge.cpf:ro" \
  -v "$PWD/scripts/iris.init:/usr/irissys/iris.init:ro" \
  intersystemsdc/iris-community:2026.2
```

**Pin the engine version** — never use `latest` (it silently drifted 2026.1 → 2026.2 and
broke dialect assumptions). Re-validate on any engine upgrade.

---

## 4. Embedded Python UDF Deployment Contract

EPython UDFs load `irispark_udc.py` inside IRIS's embedded Python interpreter. The
deployment contract is:

- **External Dockerfile** (recommended): copy the UDC module into the IRIS image and set
  the path:
  ```dockerfile
  COPY ... /opt/irispark/irispark_udc.py
  ENV IRISPARK_UDC_PATH=/opt/irispark/irispark_udc.py
  ```
- **Auto-probe**: IrisPark probes `/repo/...` (dev container mount) → client-side
  `_UDC_PATH` (same-machine IRIS) via a throwaway `LANGUAGE PYTHON` probe function.
- **Env override**: `IRISPARK_UDC_PATH` wins without probing.
- **Graceful degradation**: if no candidate exists, IrisPark warns and skips installation —
  it never breaks session startup.

---

## 5. Configuration

### Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `IRIS_HOST` | IRIS host | `localhost` |
| `IRIS_PORT` | IRIS superserver port | `1972` |
| `IRIS_NAMESPACE` | IRIS namespace | `USER` |
| `IRIS_USERNAME` | IRIS user | `_SYSTEM` |
| `IRIS_PASSWORD` | IRIS password | `SYS` |
| `IRISPARK_UDC_PATH` | Path to `irispark_udc.py` on the IRIS host | auto-probed |
| `ISC_SSLconfigurations` | Path to SSL definitions file (TLS) | — |

### Session builder

```python
session = IrisParkSession.builder() \
    .host(...).port(...).namespace(...).username(...).password(...) \
    .timeout(...).sslconfig(...) \
    .getOrCreate()
```

`getOrCreate()` reuses the active session only if all config keys match; a mismatch raises
`ValueError` (PySpark-compatible).

---

## 6. Verification

Run the deployment diagnostic before going live:

```bash
irispark-doctor
```

It checks IRIS connection + version, Python version, IrisPark version, platform
architecture, CPU flags (AVX/AVX2/BMI/BMI2), and columnar/vector support, then reports
`READY` or `CHECK FAILED`.

---

## 7. CI Pipeline

The repo's CI (`.github/workflows/ci.yml`) runs three jobs on `main` and PRs:

| Job | What it does |
|---|---|
| `lint` | `ruff check` + `mypy` on Python 3.12 |
| `test-offline` | `pytest tests/` on Python 3.10/3.11/3.12 (no IRIS) |
| `test-online` | Starts a pinned IRIS 2026.2 container, seeds test data, runs `pytest tests/ -m ""` |

---

## 8. Production Checklist

- [ ] `irispark-doctor` reports `READY`.
- [ ] IRIS engine version pinned (no `latest`).
- [ ] EPython UDF path set via `IRISPARK_UDC_PATH` or the external Dockerfile contract.
- [ ] TLS enabled (`sslconfig`) for the IRIS connection.
- [ ] Credentials from environment variables, not source control.
- [ ] Foreign table passwords use named JDBC connections or IRIS-side credential stores
  (see [Security Guide](security_guide.md)).
- [ ] Performance baselines recorded (see [Performance Guide](performance_guide.md)).
- [ ] Known differences reviewed (see [Known Differences](known_differences.md)).
