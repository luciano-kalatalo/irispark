# Architecture Guide

This guide describes IrisPark's architecture: the layered design, the lazy SQL generator,
the capability registry, and the execution model.

---

## 1. Layered Architecture

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

**Key insight:** IrisPark does **not** build a query optimizer. IRIS already has a
world-class cost-based optimizer with predicate pushdown, join reordering, and index
selection. IrisPark's job is to generate clean SQL and expose IRIS capabilities through a
familiar API.

---

## 2. Design Principles

1. **DataFrame-first experience** — fluent PySpark-like API.
2. **IRIS-first execution** — all operations push down to IRIS SQL.
3. **Lazy evaluation** — transformations build a DAG; actions trigger execution.
4. **SQL transparency** — `df.to_sql()` shows generated SQL; `df.explain()` shows the IRIS plan.
5. **Simplicity over feature parity** — focus on common analytics patterns.
6. **Native IRIS extensions** — expose IRIS capabilities through the DataFrame API.

---

## 3. Execution Model

### Engine selection

The planner selects an execution engine per operation, in preference order:

```
Native SQL → ObjectScript UDF → ObjectScript UDAF → Embedded Python
```

Python is an analytical extension, not the default execution engine. The decision tree
(`rules_functions_aggregations.md` §12):

```
OPERATION → Available in IRIS SQL?
  YES → SQL
  NO  → Scalar? → ObjectScript UDF
        Aggregate? → SQL/UDAF
        Scientific algorithm? → Embedded Python
        else → Evaluate
```

### Logical vs Physical plan

Operations are first represented as logical nodes (e.g. `Aggregate → GroupBy: state →
Avg(income)`), then the planner maps each to a physical execution (e.g. `Avg(income)` →
`SQL AVG(income)`; `Correlation(x,y)` → `IRISPARK.CORR(x,y)`).

---

## 4. Capability Registry

`irispark/registry.py` holds a `FunctionDefinition` dataclass for every registered
function:

- `name`, `pyspark_name`, `category`
- `execution` — `native_sql` / `sql_composition` / `objectscript` / `embedded_python` /
  `python_fallback`
- `compatibility` — levels A–E
- `status`, `columnar_friendly`, `vector_candidate`, `objectscript_fallback`,
  `python_fallback`, `notes`

~120 functions are registered across math, string, datetime, aggregate, window, udc, udaf,
conditional, and misc categories, plus 10 IRISPARK-native aggregates.

The registry drives:

- `df.explain(extended=True)` — prints the `[Function Registry]` section.
- `docs/compatibility.md` — the auto-generated compatibility matrix.
- `docs/migration.md` — the auto-generated migration guide.

---

## 5. Observability

- **`df.explain()`** — emits the §41 structure: `[Logical Plan]`, `[Execution Mapping]`
  (lineage op → engine), `[Source]` (table + storage mode), `[Fallback]` (ObjectScript:
  YES/NO, Python: YES/NO), `[Pushdown]` (percentage), `[IRIS Explain Plan]`.
- **`df.lineage(show=True)`** — transformation history.
- **`df.to_sql()`** — generated SQL.
- **Session metrics** — opt-in via `session.config("irispark.observability", True)`; records
  per-query query text, elapsed time, and rows returned.
- **`irispark-doctor`** — deployment diagnostic (connection, version, CPU flags, columnar
  support).

---

## 6. Data Access & Federation

IrisPark supports foreign tables for external data access:

- **JDBC foreign tables** (`read.jdbc()`) — register an IRIS Foreign Server + Foreign Table
  pointing at a remote JDBC table.
- **File-based foreign tables** (`read.parquet(foreign=True)`, `read.csv(foreign=True)`) —
  register a file-backed foreign server for local or S3-style paths.
- **Cross-source joins** — an IRIS table and a foreign table join in a single pushed-down
  SQL query.
- **Write-back** — `df.write.jdbc()` and `df.write.saveAsForeignTable()`.

See the [Foreign Tables Guide](foreign_tables_guide.md) for details.

---

## 7. Long-Term Architecture

The long-term architecture (scope §87) adds a Capability Planner between the Logical
Planner and execution, routing to IRIS-native data, enterprise data (foreign), and object
storage, all through the IRIS optimizer with ROW and COLUMNAR execution paths.

The durable differentiation is:

> **PySpark compatibility + enterprise data locality + native IRIS SQL + capability-aware
> planning + columnar/vectorized execution + federation + Python interoperability +
> transparent execution.**
