# Release Notes

Summary of IrisPark releases. For the full detail, see the [CHANGELOG](../CHANGELOG.md).

---

## Unreleased (next)

## v1.6.0 — 2026-08-21

- **Production documentation set (§78)** — added the missing deliverables: Getting Started,
  Known Differences, Security Guide, Deployment Guide, Performance Guide, Architecture
  Guide, Foreign Tables Guide, Troubleshooting, Upgrade Guide, Release Notes, API Reference.
- **Production-readiness gates (§75/§74)** — formal security review artifact (APPROVED WITH
  CONDITIONS) and committed benchmark baseline for regression detection.
- **Docs** — populated `docs/PATTERNS.md` and `docs/LESSONS_LEARNED.md` (were empty stubs);
  renamed `product_view.md` → `PROJECT_SCOPE.md`.
- **Versioning** — aligned to v1.6.0 across `pyproject.toml`, README, and docs.

## v1.5.0 — 2026-08-14

- **PySpark pandas I/O parity (Tier-1)** — module-level `read_csv`, `read_parquet`,
  `read_json`, `read_table`, `read_sql`, `read_sql_query`, `read_sql_table`, `from_pandas`
  in `irispark.io`; new `Read.json` reader.
- **Phase 1–6 gap-closure + DS 0.3 fixes** (merged in `832c865`):
  - Phase 1: EPython UDFs fully wired + tested.
  - Phase 2: Capability Registry + compatibility docs.
  - Phase 3: `explain()` §41 structure.
  - Phase 4: Observability + `irispark-doctor` CLI.
  - Phase 5: API conformance — `Row` class, `toPandas` alias, namespace imports.
  - Phase 6: Data access & security — TLS passthrough, password-in-DDL warnings, live
    certification tests, Arrow claim fix.
- **Fixes**: `timeout=None`/`sslconfig=None` regression, `getOrCreate` session reuse,
  `na_drop` NULL semantics, `unionByName` implementation, `format_string`/`printf`/`conv`/
  `parse_url` UDFs, edge-case parity closes.

## v1.4.0 — 2026-08-14

- **PySpark GroupedData parity (Tier-1)** — `mean`, `pivot(values=None)` auto-distinct,
  `agg` empty guard.

## v1.3.0 — 2026-08-14

- **PySpark Column API parity (Tier-1)** — 12 new `Column` methods: `astype`, `name`,
  `substr`, `when/otherwise`, `isNaN`, `eqNullSafe`, `ilike`, `asc/desc_nulls_first/last`.

## v1.2.0 — 2026-08-14

- **IRIS diagnostics** — `df.iris.show_stats()`, `show_indexes()`, `suggest_indexes()`
  rewritten on the IRIS 2026.x schema; first test coverage for the diagnostics APIs.

## v1.1.0 — 2026-08-14

- **DS 0.1 / DS 0.2 / DS 0.3** — JDBC foreign tables, write-back & cross-source
  federation, file-based foreign tables.
- **MIT License**, CI `test-online` job, `session.iris` / `df.iris.foreign` namespaces.
- Many SQL-generation fixes (POWER, reserved-word quoting, COALESCE, isnull context forms,
  corr formula).

## v1.0.0 — 2026-08-12

- **Core MVP** — DataFrame API, SQL pushdown, lazy evaluation, window functions, ML
  transformers, RDD API, read/write (parquet, csv, jdbc).
- **v1.1 PySpark compatibility layer** — `df.na`, `df.stat`, `withColumns`,
  `withColumnsRenamed`, `unionByName`, `transform`, `colRegex`, `toDF`.
- **`df.iris` namespace** — full IRIS diagnostics.
