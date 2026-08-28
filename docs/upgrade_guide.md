# Upgrade Guide

This guide covers upgrading IrisPark and its IRIS engine, and the compatibility
assessment required when a new PySpark version is released.

---

## 1. Upgrading the IrisPark Python package

```bash
pip install --upgrade irispark
```

### Breaking changes

Review the [CHANGELOG](../CHANGELOG.md) for breaking changes between versions. Notable
historical breaking changes:

- **v1.1** — `read.jdbc()` switched from local temp-table ingestion to foreign tables;
  `docker-compose.yml` removed.
- **v1.2** — `df.iris.show_indexes()`/`suggest_indexes()`/`show_stats()` rewritten on the
  IRIS 2026.x `INFORMATION_SCHEMA` schema.
- **v1.5** — `pyspark.pandas.io` module functions require an explicit `session=` or an
  active session.

### After upgrading

1. Run `irispark-doctor` to confirm the deployment is healthy.
2. Run the test suite (`pytest tests/ -m ""`) against your IRIS instance.
3. Re-run the benchmark suite and diff against the baseline (see
   [Performance Guide](performance_guide.md)).

---

## 2. Upgrading the IRIS engine

**Never use `latest`** — it silently drifted 2026.1 → 2026.2 and broke dialect
assumptions. Pin the engine and re-validate on any upgrade.

### Procedure

1. Pin the new engine version (e.g. `intersystemsdc/iris-community:2026.2`).
2. Re-run the full online test suite.
3. Re-run the benchmark suite and diff against the baseline.
4. Re-verify dialect-dependent features:
   - `groupingSets` — re-check on each IRIS release (2026.1: SQLCODE -29; 2026.2: parses
     `ROLLUP`/`CUBE`/`GROUPING SETS` as function/field references).
   - `%PARALLEL` hint placement (`FROM %PARALLEL <table>`).
   - `INFORMATION_SCHEMA.INDEXES` schema (used by `show_indexes`).
   - `%SYS.GlobalQuery*` SQL API (removed in IRIS 2026.x — storage keys omitted).

---

## 3. PySpark version compatibility

A new PySpark version triggers a **compatibility assessment**, not an automatic
compatibility claim (scope §77). When PySpark releases a new version:

1. Run the parity harness (`scripts/parity.py`) against the new PySpark.
2. Identify new functions and changed semantics.
3. Update the [Compatibility Matrix](compatibility.md) and [Migration Guide](migration.md).
4. Update `non_covered.md` for newly-covered or newly-absent functions.

---

## 4. Rollback

- **Python package**: `pip install irispark==<previous-version>`.
- **IRIS engine**: restore the previous pinned image and re-validate.
- **Durable data**: the dev environment keeps durable data in `ISC_DATA_DIRECTORY`; a
  pre-swap backup is recommended before engine upgrades (the project keeps backups under
  `irispark-env/irispark-bkp/`).
