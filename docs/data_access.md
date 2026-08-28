# IrisPark Data Access Guide

This document describes how to connect IrisPark to external data sources using IRIS Foreign Tables (FDW) and JDBC gateways.

## 1. PostgreSQL

### Live Certification
Tested against PostgreSQL 16 with IRIS 2026.2 JDBC gateway.

### Setup
1. Download PostgreSQL JDBC driver (e.g., `postgresql-42.7.4.jar` from Maven Central).
2. Place the JAR in the IRIS container's JDBC gateway classpath:
   ```bash
   docker cp postgresql-42.7.4.jar iris:/usr/irissys/dev/java/lib/JDK18/
   docker exec iris iris session IRIS -U %SYS "do ##class(%Net.Remote.Java.JDBCGateway).%Restart()"
   ```
   Or add to the `%JDBC Server` gateway classpath in iris.cpf:
   ```ini
   [Gateways]
   %JDBC Server=JDBC,53772,%Gateway_SQL,/usr/irissys/dev/java/lib/JDK18/postgresql-42.7.4.jar,,,,,0
   ```
   Then restart IRIS.

### Connection
```python
from irispark import IrisParkSession

session = IrisParkSession.builder() \
    .host("localhost") \
    .port(5433) \
    .namespace("DATASPARK") \
    .username("irispark") \
    .password("irispark") \
    .getOrCreate()

# Option 1: Full JDBC URL with credentials
df = session.read.jdbc(
    url="jdbc:postgresql://localhost:5433/irispark_test",
    dbtable="sales",
    user="irispark",
    password="irispark",
    driver="org.postgresql.Driver"
)

# Option 2: Named connection (preferred for security)
# 1. Create named connection in IRIS: CREATE JDBC CONNECTION pg_sales ...
# 2. Use CONNECTION option:
df = session.read.jdbc(
    url="jdbc:postgresql://localhost:5433/irispark_test",
    dbtable="sales",
    options={"CONNECTION": "pg_sales"}  # named connection in %Library.JDBCCatalog
)
```

### Verified Patterns
| Pattern | Status |
|---|---|
| `read.jdbc()` with full URL + credentials | ✅ Certified |
| `read.jdbc()` with named CONNECTION | ✅ Certified (recommended for security) |
| `df.write.jdbc()` | ✅ Certified |

---

## 2. Oracle Database

### Live Certification
Tested against Oracle 23c Free (gvenzl/oracle-free:23-slim) with IRIS 2026.2.

### Setup
1. Download Oracle JDBC driver (`ojdbc11.jar` from Oracle Maven or Oracle website).
2. Place in IRIS JDBC gateway classpath (same as PostgreSQL).
3. Restart JDBC gateway: `##class(%Net.Remote.Java.JDBCGateway).%Restart()`

### Connection
```python
df = session.read.jdbc(
    url="jdbc:oracle:thin:@//localhost:1521/FREEPDB1",
    dbtable="SALES",
    user="irispark",
    password="irispark",
    driver="oracle.jdbc.OracleDriver"
)
```
Or with named connection (recommended):
```python
df = session.read.jdbc(
    url="jdbc:oracle:thin:@//localhost:1521/FREEPDB1",
    dbtable="SALES",
    options={"CONNECTION": "oracle_sales"}
)
```

### Verified Patterns
| Pattern | Status |
|---|---|
| `read.jdbc()` with full URL + credentials | ✅ Certified |
| `read.jdbc()` with named CONNECTION | ✅ Certified (recommended) |

---

## 3. Microsoft SQL Server

### Status
⚠️ **Not live-certified** on arm64 hosts. The official Microsoft JDBC driver (`mssql-jdbc`) runs on amd64 only. On Apple Silicon / arm64 Linux hosts, the container runs under emulation and is unstable.

### Recommended Workaround
- Run SQL Server on a separate amd64 machine / VM.
- Use `read.jdbc()` with the remote host/port.
- Or use `read.jdbc()` from an amd64 client machine connecting to SQL Server.

```python
# From an amd64 machine:
df = session.read.jdbc(
    url="jdbc:sqlserver://sqlserver-host:1433;databaseName=irispark_test",
    dbtable="sales",
    user="sa",
    password="YourStrong!Passw0rd",
    driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
)
```

---

## 3. TLS / SSL Connections

### Server-Side (IRIS Superserver TLS)

1. Generate certificates:
   ```bash
   openssl req -x509 -newkey rsa:2048 -keyout server.key -out server.crt -days 365 -nodes -subj "/CN=localhost"
   openssl req -x509 -newkey rsa:2048 -keyout ca.key -out ca.crt -days 365 -nodes -subj "/CN=IRISPark-CA"
   ```

2. Create merge CPF for IRIS superserver TLS (`merge-tls.cpf`):
   ```ini
   [Startup]
   SSLCertificateFile=/certs/server.crt
   SSLCAFile=/certs/ca.crt
   ```

3. Build IRIS TLS image:
   ```dockerfile
   FROM intersystemsdc/iris-community:2026.2
   COPY merge-tls.cpf /config/merge.cpf
   COPY certs/ /certs/
   ```

4. Run container on port 1973 (superserver TLS) with volumes for certs and merge.cpf.

### Client-Side (IrisPark)

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

**Note**: Server-side TLS on IRIS superserver requires `SSLCertificateFile` and `SSLPrivateKeyFile` in `[Startup]` section of merge.cpf. The `SSLCAFile` parameter is accepted in IRIS 2026.2 but may be ignored for superserver TLS (only client cert verification uses it). Tested working with self-signed CA + server cert on IRIS 2026.2.

---

## 4. MinIO / S3 Object Storage

### Status: Deferred
IRIS 2026.2 built-in CSV/PARQUET/JSON foreign data wrappers (`%SQL.FDW.CSV`, `%SQL.FDW.PARQUET`, `%SQL.FDW.JSON`) do **not** currently support S3 endpoint configuration via standard `s3_*` options. The `s3_endpoint` / `s3_access_key_id` options accepted by `register_file_foreign_table()` are passed to the IRIS foreign server but the built-in CSV/PARQUET wrappers ignore them.

### Workaround: Custom FDW (IRISpark.FDW.Parquet)
Use the custom FDW from the companion project **iris-parquet** (separate repo):
```bash
# Deploy custom FDW classes to IRIS
docker cp src/IRISpark/FDW/Parquet.cls <iris>:/tmp/Parquet.cls
docker cp src/IRISpark/FDW/Server.cls <iris>:/tmp/Server.cls
docker cp src/python/parquet_reader.py <iris>:/external/durable/mgr/python/

docker exec <iris> sh -c 'iris session IRIS -U %SYS "do ##class(%SYSTEM.OBJ).Load(\"/tmp/Parquet.cls\",\"ck\")"'
docker exec iris session IRIS -U LAKE <<'EOF'
do ##class(Security.SSLConfigs).Create("minio-tls", "MinIO TLS", "/certs/ca.crt", "", "", "", 0, "", "TLSv1.2:TLSv1.3", "HIGH", 0, 0)
write "SSL Config created: ", $SYSTEM.Status.GetErrorText($SYSTEM.Status.GetErrorCode()), !
halt
EOF
```

Then use via:
```python
df = session.read.parquet(
    "s3://analytics/sales/",
    foreign=True,
    options={
        "s3_endpoint": "http://host.docker.internal:9000",
        "s3_access_key_id": "admin",
        "s3_secret_access_key": "admin123",
        "s3_bucket": "analytics"
    },
    format="parquet"  # uses IRISpark.FDW.Parquet wrapper
)
```

### Status
| Source | Status |
|---|---|
| `read.csv(foreign=True, options={"s3_endpoint": ...})` | ⚠️ Not supported by built-in CSV FDW |
| `read.parquet(foreign=True, options={"s3_endpoint": ...})` | ⚠️ Not supported by built-in wrapper |
| Custom `IRISpark.FDW.Parquet` FDW | ✅ Working (separate project) |

**Recommendation**: For production S3/MinIO access, deploy the `IRISpark.FDW.Parquet` FDW from the `iris-parquet` companion project.

---

## 4. Security — Credential Handling

### Password in DDL Warning
When using `session.read.jdbc()` with explicit `password`, the password is emitted into the generated `CREATE FOREIGN SERVER ... OPTIONS (password '...')` DDL. IRIS 2026.2 does **not** provide a credential vault for foreign server passwords — the password is stored in plaintext in the foreign server definition.

**Mitigation**:
1. **Named JDBC connection** (recommended):
   ```sql
   CREATE FOREIGN SERVER pg_sales FOREIGN DATA WRAPPER %SQL.FDW.XDBC
       OPTIONS (CONNECTION 'pg_sales');
   ```
   Then in Python:
   ```python
   df = session.read.jdbc(url="jdbc:postgresql://host:5432/db", dbtable="t", options={"CONNECTION": "pg_sales"})
   ```
2. **IRIS-side credential storage**: Use `%Library.JDBCCatalog` to store connection properties server-side, then reference by name.
3. **Session-level config**: Avoid embedding passwords in DDL by using IRIS-side credential stores when available.

IrisPark emits a `UserWarning` when a password is passed to `register_jdbc_foreign_table()` / `read.jdbc()` to alert users.

---

## 5. Results & Arrow Interop

IrisPark returns results as `Row` objects (tuple-like with attribute access). Conversion to Arrow / pandas / Polars:

```python
# To pandas
pdf = df.to_pandas()          # client-side Arrow bridge (not zero-copy)
pdf = df.to_pandas()          # alias

# To Polars
import polars as pl
pl_df = df.to_polars()

# To Arrow
batch = df.to_arrow()
import pyarrow as pa
table = pa.Table.from_batches([batch])

# To Dask
import dask.dataframe as dd
ddf = dd.from_pandas(df.to_pandas(), npartitions=4)
```

**Note**: Results materialize as Arrow RecordBatch **client-side** (Python process). The "zero-copy" claim in earlier docs was inaccurate — data is copied from IRIS wire protocol → Python tuples → Arrow arrays → pandas/Polars. This is still fast (network → Python → Arrow → pandas) but not zero-copy in the strict sense.

---

## 6. Connection Parameters

| Parameter | Type | Description |
|---|---|---|
| `host` | str | IRIS host |
| `port` | int | Superserver port (default 1972) |
| `namespace` | str | IRIS namespace (default: `DATASPARK`) |
| `username` | str | IRIS username |
| `password` | str | IRIS password |
| `timeout` | int | Connection timeout in seconds (default: 10) |
| `sslconfig` | str | SSL configuration name from SSL definitions file (see §3) |

### Builder Example
```python
session = IrisParkSession.builder() \
    .host("localhost") \
    .port(1972) \
    .namespace("DATASPARK") \
    .username("suser") \
    .password("pass123") \
    .timeout(30) \
    .sslconfig("irispark-tls") \
    .getOrCreate()
```

### getOrCreate Behavior
`builder.getOrCreate()` reuses an existing session **only if all config keys match** (required + optional: `host`, `port`, `namespace`, `username`, `password`, `timeout`, `sslconfig`). Mismatch raises `ValueError` (PySpark-compatible behavior).

---

## 9. Troubleshooting

| Issue | Resolution |
|---|---|
| `ClassNotFound: org.postgresql.Driver` | JDBC driver not in IRIS gateway classpath. Add JAR to `/usr/irissys/dev/java/lib/JDK18/` and restart JDBC gateway. |
| `ORA-12514: TNS:listener does not currently know of service` | Check service name (`FREEPDB1` for Oracle Free). Use `jdbc:oracle:thin:@//host:1521/FREEPDB1`. |
| `SSL handshake failed` | Check `ISC_SSLconfigurations` path, verify `CAFile`/`CertFile`/`KeyFile` paths in SSL settings file, ensure server cert CN matches hostname. |
| `Foreign server password visible in DDL` | Use named JDBC connection (`CONNECTION` option) or IRIS-side credential storage. |
| `ORA-12514` on Oracle | Ensure service name matches (`FREEPDB1` for gvenzl/oracle-free). |
| `MSSQL connection failed` | On arm64, use `platform: linux/amd64` in compose (slow) or remote amd64 host. |

---

*Last updated: 2026-08-19 (IrisPark Phase 6 release)*