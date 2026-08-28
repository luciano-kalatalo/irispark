# Security Guide

This guide covers the security model of IrisPark and how to deploy it securely. It
complements the [Security Policy](../../SECURITY.md) (vulnerability reporting) and the
credential/TLS details in [`data_access.md`](data_access.md).

---

## 1. Security Model

IrisPark is a **client-side library** that connects to InterSystems IRIS over the DB-API
driver and generates SQL. Its security posture rests on three layers:

1. **Input validation** — all user inputs are validated before SQL generation.
2. **IRIS-side security** — authentication, authorization, TLS, and credential storage are
   owned by IRIS.
3. **Network security** — the connection between IrisPark and IRIS must be protected
   (TLS/VPN).

IrisPark does **not** implement its own authentication or authorization; it delegates to
IRIS.

---

## 2. Input Validation & SQL Injection Prevention

IrisPark applies defense-in-depth against SQL injection:

- **Whitelist regex validation** — table/column names are validated against strict regex
  patterns before SQL generation.
- **Identifier validation** — schema-qualified identifiers are checked.
- **Parameterized/escaped values** — user values are properly quoted/escaped.
- **No dynamic code execution** — no `eval()`, `exec()`, or dynamic code generation from
  user input.

**Rule**: never pass raw user input into `session.sql()` or string predicates without
validation. Prefer the DataFrame API (`filter`, `select`, `where`) which validates
identifiers, over raw SQL strings.

---

## 3. Connection Security (TLS)

### Client-side TLS

```python
import os
os.environ["ISC_SSLconfigurations"] = "/path/to/ssl-defs.ini"

session = IrisParkSession.builder() \
    .host("localhost") \
    .port(1973) \
    .namespace("DATASPARK") \
    .username("suser") \
    .password("pass123") \
    .sslconfig("irispark-tls") \
    .getOrCreate()
```

SSL definitions file (`ssl-defs.ini`):

```ini
[irispark-tls]
Address=localhost
Port=1973
SSLConfig=irispark-tls

[irispark-tls]
CAFile=/path/to/ca.crt
CertFile=/path/to/client.crt
KeyFile=/path/to/client.key
VerifyPeer=0
```

### Server-side TLS

IRIS superserver TLS requires `SSLCertificateFile` and `SSLPrivateKeyFile` in the
`[Startup]` section of `merge.cpf`. See [`data_access.md`](data_access.md) §3 for the full
certificate generation and container setup. Tested working with self-signed CA + server
cert on IRIS 2026.2.

**Note**: `timeout` and `sslconfig` are optional; when `None` they are omitted from the
connection kwargs (they must not be forwarded to the driver).

---

## 4. Credential Handling

### 4.1 IRIS connection credentials

- Credentials are passed to the IRIS DB-API driver; IrisPark does **not** log or store
  them in plain text.
- Prefer environment variables (`.env` via `python-dotenv`) over hardcoded credentials.
- `.env*` files are gitignored to prevent accidental commits.

### 4.2 Foreign table passwords (JDBC / file)

**Warning**: When using `session.read.jdbc()` with an explicit `password`, the password is
emitted into the generated `CREATE FOREIGN SERVER ... OPTIONS (password '...')` DDL. IRIS
2026.2 does **not** provide a credential vault for foreign server passwords — the password
is stored in **plaintext** in the foreign server definition.

IrisPark emits a `UserWarning` when a password is passed to
`register_jdbc_foreign_table()` / `read.jdbc()` to alert you.

**Mitigations** (in order of preference):

1. **Named JDBC connection** (recommended) — store connection properties server-side and
   reference by name:
   ```sql
   CREATE FOREIGN SERVER pg_sales FOREIGN DATA WRAPPER %SQL.FDW.XDBC
       OPTIONS (CONNECTION 'pg_sales');
   ```
   ```python
   session.read.jdbc(dbtable="public.accounts", connection="pg_sales")
   ```
2. **IRIS-side credential storage** — use `%Library.JDBCCatalog` to store connection
   properties server-side, then reference by name.
3. **Session-level config** — avoid embedding passwords in DDL by using IRIS-side
   credential stores when available.

---

## 5. Object Storage (S3 / MinIO)

- IRIS 2026.2 built-in CSV/PARQUET/JSON foreign data wrappers do **not** support S3
  endpoint configuration via standard `s3_*` options.
- For production S3/MinIO access, deploy the `IRISpark.FDW.Parquet` FDW from the
  `iris-parquet` companion project. See [`data_access.md`](data_access.md) §4.
- S3 credentials passed via `options` are subject to the same plaintext-in-DDL caveat as
  JDBC passwords.

---

## 6. Session Lifecycle & Cleanup

- Temporary tables and foreign servers/tables created during operations are cleaned up on
  `session.close()`.
- Foreign tables/servers created by `read.jdbc()` / file reads are dropped on
  `session.close()`.
- Persistent foreign tables are opt-in with `persistent=True` — use only for shared
  Bronze/Silver views you intend to keep.

---

## 7. Deployment Checklist

- [ ] IRIS connection uses TLS (`sslconfig`) in production.
- [ ] Credentials come from environment variables, not source control.
- [ ] No `.env*` files committed.
- [ ] Foreign table passwords use named JDBC connections or IRIS-side credential stores.
- [ ] `irispark-doctor` reports `READY`.
- [ ] IRIS engine version is pinned (no `latest`).
- [ ] Network access to IRIS is restricted (firewall/VPN).
