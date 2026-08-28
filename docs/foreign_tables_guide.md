# Foreign Tables Guide

This guide covers IrisPark's foreign table support (Data Services DS 0.1 / DS 0.2 /
DS 0.3): reading and writing external data through IRIS Foreign Servers and Foreign
Tables, with no local data copy.

---

## 1. Overview

Foreign tables let IRIS own the connection to external data. IrisPark registers an IRIS
Foreign Server + Foreign Table pointing at a remote source; IRIS queries it directly and
rows are **not** copied into Python.

Three data-service tiers:

| Tier | Capability |
|---|---|
| **DS 0.1** | JDBC foreign tables — `read.jdbc()` |
| **DS 0.2** | Foreign table write-back & cross-source federation |
| **DS 0.3** | File-based foreign tables — `read.parquet(foreign=True)`, `read.csv(foreign=True)` |

---

## 2. Reading External Data

### JDBC

```python
df = iris.read.jdbc(
    url="jdbc:postgresql://dbserver/sourcedb",
    dbtable="public.accounts",
    user="analyst",
    password="...",
    driver="org.postgresql.Driver",
)
```

Requires the `jdbc` extra (`sqlalchemy`). Schema-qualified remote table names
(`schema.table`) are supported.

### Files (local or S3-style)

```python
# Local
df = iris.read.parquet("data.parquet", foreign=True)
df = iris.read.csv("data.csv", foreign=True, options={"header": True})

# S3-style URI
df = iris.read.parquet("s3://bucket/data.parquet", foreign=True)
```

Schema is inferred from the file via pyarrow so the emitted `CREATE FOREIGN TABLE` DDL is
typed.

---

## 3. Writing External Data

```python
# Write back through a foreign table (INSERT INTO ... SELECT ...)
df.write.jdbc(
    url="jdbc:postgresql://dbserver/sourcedb",
    dbtable="public.scores",
    user="analyst",
    password="...",
    driver="org.postgresql.Driver",
    mode="overwrite",
)

# Publish a DataFrame to an existing foreign server
df.write.saveAsForeignTable()
```

---

## 4. Cross-Source Federation

An IRIS table and a foreign table join in a **single pushed-down SQL query** — no local
data movement:

```python
local = iris.table("sales")
remote = iris.read.jdbc(url=..., dbtable="public.accounts", ...)
joined = local.join(remote, "account_id")
```

---

## 5. Session Lifecycle

- Foreign tables/servers created by `read.jdbc()` / file reads are **dropped on
  `session.close()`**.
- **Persistent** foreign tables are opt-in with `persistent=True` for shared Bronze/Silver
  views.

---

## 6. `session.iris` Namespace

| Method | Purpose |
|---|---|
| `register_jdbc_foreign_table(...)` | Register a JDBC foreign table |
| `register_file_foreign_table(...)` | Register a CSV/Parquet/JSON file foreign table |
| `create_foreign_table_from_query(...)` | Create a foreign table from a query |
| `drop_foreign_table(name)` | Drop a foreign table |
| `foreign_tables()` | List tracked foreign tables |

---

## 7. `df.iris.foreign` Namespace

| Method | Purpose |
|---|---|
| `is_foreign_table()` | Is this DataFrame backed by a foreign table? |
| `server_name()` | The backing foreign server name |
| `is_persistent()` | Is the foreign table persistent? |
| `refresh()` | Refresh the foreign table definition |

---

## 8. Security Considerations

- **Passwords in DDL**: `read.jdbc()` with an explicit `password` emits it into the
  `CREATE FOREIGN SERVER ... OPTIONS (password '...')` DDL. IRIS 2026.2 has **no**
  credential vault for foreign server passwords — it is stored in plaintext. IrisPark emits
  a `UserWarning`. Prefer **named JDBC connections** or IRIS-side credential storage
  (`%Library.JDBCCatalog`). See the [Security Guide](security_guide.md).
- **S3/MinIO**: IRIS 2026.2 built-in CSV/PARQUET/JSON wrappers do not support S3 endpoint
  config via `s3_*` options. For production S3 access, deploy the `IRISpark.FDW.Parquet`
  FDW from the `iris-parquet` companion project. See [`data_access.md`](data_access.md) §4.

---

## 9. Certified Sources

| Source | Status |
|---|---|
| PostgreSQL (JDBC) | ✅ Certified (round-trip on 5433) |
| Oracle (JDBC) | ✅ Certified (round-trip on 1521) |
| SQL Server | ⚠️ Skipped in CI (amd64 image on arm64 host — documented) |
| MinIO / S3 | ⚠️ Deferred — custom `IRISpark.FDW.Parquet` FDW required |
| Local Parquet / CSV / JSON | ✅ Certified |
