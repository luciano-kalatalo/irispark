# Security Review

**Status**: Approved (with conditions)
**Date**: 2026-08-21
**Scope**: IrisPark v1.6.0 — client-side PySpark-compatible analytics engine for
InterSystems IRIS.
**Gate**: Satisfies §75 "security review approved" production-readiness condition.

---

## 1. Review Scope

This review covers the security-relevant surfaces of IrisPark:

1. SQL injection prevention (input validation before SQL generation).
2. Credential handling (IRIS connection + foreign table passwords).
3. Network security (TLS).
4. Session lifecycle & cleanup.
5. Supply chain / dependencies.
6. Known limitations and residual risk.

---

## 2. Findings

### 2.1 SQL Injection Prevention — PASS

IrisPark applies defense-in-depth before generating SQL:

| Control | Implementation | Status |
|---|---|---|
| Identifier validation | `_IDENTIFIER_RE = ^[a-zA-Z_][a-zA-Z0-9_.]*$` applied via `_validate_identifier()` to table/column/order-column names (`sql_generator.py:81`) | ✅ |
| Filter validation | `_SAFE_FILTER_RE` whitelist applied via `_validate_filter()` to string predicates (`sql_generator.py:86`) | ✅ |
| LIMIT validation | `_SAFE_LIMIT_RE = ^\d+$` applied via `_validate_limit()` (`sql_generator.py:103`) | ✅ |
| Value quoting | User values passed through `_quote()` (string escaping) | ✅ |
| No dynamic code execution | No `eval()`/`exec()`/dynamic code generation from user input | ✅ |

**Residual risk (accepted)**: `session.sql()` and raw-SQL string predicates accept arbitrary
SQL by design. This is a documented power-user escape hatch, not a vulnerability — callers
must not pass untrusted input to these paths. The DataFrame API (`filter`, `select`,
`where`) validates identifiers and is the safe path.

### 2.2 Credential Handling — PASS WITH CONDITIONS

| Surface | Finding | Status |
|---|---|---|
| IRIS connection credentials | Passed to the DB-API driver; not logged or stored in plaintext by IrisPark | ✅ |
| `.env` files | `.env*` gitignored to prevent accidental commits | ✅ |
| Foreign table passwords | Emitted into `CREATE FOREIGN SERVER ... OPTIONS (password '...')` DDL; IRIS 2026.2 has **no** credential vault → stored in plaintext in the foreign server definition | ⚠️ |
| Password-in-DDL warning | `UserWarning` emitted when a password is passed to `register_jdbc_foreign_table()` / `read.jdbc()` (`session_iris_extensions.py:152`) | ✅ |

**Condition**: Foreign table passwords must use a **named JDBC connection** (`CONNECTION`
option) or IRIS-side credential storage (`%Library.JDBCCatalog`) in production. The
plaintext-in-DDL behavior is a documented IRIS 2026.2 limitation, not an IrisPark defect.

### 2.3 Network Security (TLS) — PASS

- Client-side TLS via `sslconfig` + `ISC_SSLconfigurations` (SSL definitions file).
- Server-side TLS on the IRIS superserver via `merge.cpf` (`SSLCertificateFile`/
  `SSLPrivateKeyFile`).
- Tested working with self-signed CA + server cert on IRIS 2026.2.
- `timeout`/`sslconfig` are optional; `None` values are omitted from connection kwargs
  (no driver misconfiguration).

**Condition**: TLS must be enabled in production; the connection between IrisPark and IRIS
must be protected (TLS/VPN).

### 2.4 Session Lifecycle & Cleanup — PASS

- Temporary tables and foreign servers/tables created during operations are cleaned up on
  `session.close()`.
- Foreign tables/servers created by `read.jdbc()` / file reads are dropped on
  `session.close()`.
- Persistent foreign tables are opt-in with `persistent=True`.

### 2.5 Supply Chain / Dependencies — PASS WITH CONDITIONS

- Core deps: `intersystems-irispython`, `pandas`, `pyarrow`, `python-dotenv`.
- Optional: `polars`, `dask[dataframe]`, `sqlalchemy` (JDBC), `jupyter`/`ipykernel`.
- All from PyPI; no vendored binaries.

**Condition**: Pin dependency versions in production and run dependency vulnerability
scanning (e.g. `pip-audit`) as part of CI.

### 2.6 Known Limitations & Residual Risk

| Limitation | Risk | Mitigation |
|---|---|---|
| Foreign server password plaintext | Medium | Named JDBC connection / IRIS credential store |
| `session.sql()` accepts arbitrary SQL | Medium (by design) | Documented; use DataFrame API for untrusted input |
| No credential vault in IRIS 2026.2 | Medium | Documented; IRIS-side credential storage |
| S3/MinIO credentials via `options` | Medium | Same plaintext-in-DDL caveat; use `IRISpark.FDW.Parquet` |
| Non-ASCII charset negotiation | Low | Scoped to ASCII for V1; documented |

---

## 3. Verdict

**APPROVED WITH CONDITIONS.** The core security controls (input validation, no dynamic
code execution, TLS support, session cleanup, credential hygiene) are sound and satisfy the
§75 gate. The following conditions must be met for production:

1. Enable TLS on the IRIS connection.
2. Use named JDBC connections or IRIS-side credential storage for foreign table passwords.
3. Pin dependency versions and run `pip-audit` in CI.
4. Restrict network access to IRIS (firewall/VPN).
5. Do not pass untrusted input to `session.sql()` or raw-SQL string predicates.

---

## 4. Sign-off

| Role | Name | Date | Status |
|---|---|---|---|
| Security review | IrisPark maintainer | 2026-08-21 | Approved with conditions |
