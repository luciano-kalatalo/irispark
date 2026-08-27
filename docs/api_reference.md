# API Reference

This is the public API surface of IrisPark. For the full function-by-function
compatibility matrix, see [Compatibility Matrix](compatibility.md). For the migration
classification, see [Migration Guide](migration.md).

---

## Top-level exports (`irispark`)

| Symbol | Description |
|---|---|
| `IrisParkSession` | Main session class (alias of `IrisSparkSession`) |
| `IrisParkSessionBuilder` | Fluent session builder |
| `IrisSparkSession` | SCOPE-compliant alias |
| `IrisDataFrame` / `DataFrame` | DataFrame class |
| `Column` | Column expression class |
| `Row` | Tuple-like row with attribute/index/key access |
| `Window` / `WindowSpec` | Window function specification |
| `Read` | DataFrame reader (`session.read`) |
| `RDD` | In-memory RDD |
| `IrisSparkContext` | SparkContext-compatible context |
| `Accumulator` | Accumulator |
| `Catalog` | Catalog |
| `UDFRegistration` | UDF registration |
| `IrisExtensions` / `IrisForeignExtensions` | `df.iris` namespaces |
| `SessionIrisExtensions` | `session.iris` namespace |
| `SQLGenerator` / `IrisParkSQLError` | SQL generation |
| `types` | Data types module |
| `functions` | SQL functions module |

---

## Session

### `IrisParkSession.builder()`

```python
session = IrisParkSession.builder() \
    .host("localhost") \
    .port(1972) \
    .namespace("USER") \
    .username("_SYSTEM") \
    .password("SYS") \
    .timeout(...) \
    .sslconfig(...) \
    .getOrCreate()
```

`getOrCreate()` reuses the active session only if all config keys match; a mismatch raises
`ValueError`.

### Key methods

| Method | Description |
|---|---|
| `table(name)` | Read a table as a DataFrame |
| `createDataFrame(data, schema=None)` | Create a DataFrame from pandas/polars/lists |
| `sql(query)` | Run a raw SQL query |
| `read` | DataFrame reader (`Read`) |
| `sparkContext` | RDD context |
| `close()` | Close the session (clears active session, drops transient foreign tables) |
| `config(key, value)` | Set session config (e.g. `irispark.observability`) |
| `iris` | `SessionIrisExtensions` (foreign table lifecycle) |

---

## DataFrame (`IrisDataFrame`)

### Transformations (lazy)

`select`, `selectExpr`, `withColumn`, `withColumns`, `withColumnRenamed`,
`withColumnsRenamed`, `filter`/`where`, `groupBy`/`group_by`, `orderBy`/`order_by`,
`sort`, `limit`, `drop`, `dropDuplicates`/`drop_duplicates`, `distinct`, `join`,
`union`, `unionByName`, `unionAll`, `crossJoin`, `pivot`, `unpivot`/`melt`, `cube`,
`rollup`, `sample`, `randomSplit`, `transform`, `colRegex`, `toDF`, `alias`,
`na` (drop/fill/replace), `stat` (corr/crosstab/freqItems/sampleBy).

### Actions

`collect`, `first`, `take`, `head`, `tail`, `count`, `show`, `toPandas`/`to_pandas`,
`toPolars`/`to_polars`, `to_sql`, `explain`, `lineage`, `printSchema`, `schema`,
`dtypes`, `describe`, `summary`, `isEmpty`.

### `df.iris` namespace

`show_stats`, `show_indexes`, `suggest_indexes`, `explain`, `createColumnarIndex`,
`createBitmapIndex`, `tableStats`, `foreign` (is_foreign_table/server_name/is_persistent/
refresh).

---

## Column

`alias`/`name`, `cast`/`astype`, `isNull`/`isNotNull`, `isNaN`, `eqNullSafe`, `ilike`,
`like`, `when`/`otherwise`, `substr`, `asc`/`desc`, `asc_nulls_first`/`asc_nulls_last`/
`desc_nulls_first`/`desc_nulls_last`, arithmetic/comparison operators.

---

## Functions (`irispark.functions`)

~120 PySpark-compatible functions across categories:

- **Math**: `abs`, `ceil`, `floor`, `round`, `sqrt`, `pow`/`power`, `exp`, `log`, `log1p`,
  `sin`/`cos`/`tan`, `pmod`, `width_bucket`, `uniform`, and more.
- **String**: `concat`, `concat_ws`, `substring`, `upper`/`lower`, `trim`, `lpad`/`rpad`,
  `regexp_extract`/`regexp_replace`, `rlike`, `split`, `charindex`, `find_in_set`, `elt`,
  `format_string`, `printf`, `parse_url`, `initcap`, `levenshtein`, `soundex`, and more.
- **Date/time**: `dateadd`, `datepart`, `datediff`, `months_between`, `timestampdiff`,
  `to_utc_timestamp`, `from_utc_timestamp`, `trunc`, and more.
- **Conditional**: `when`, `coalesce`, `ifnull`/`nvl`, `greatest`, `least`, `nanvl`,
  `isnull`/`isnotnull`.
- **Aggregate**: `sum`, `avg`/`mean`, `min`, `max`, `count`, `count_if`, `stddev`,
  `variance`, `corr`, `covar_samp`/`covar_pop`, `collect_list`/`collect_set`, `any`/`every`/
  `some`, `bool_and`/`bool_or`, `first`/`last`, `max_by`/`min_by`, `median`, `percentile`,
  `quantile`, `percentile_approx`, `skewness`, `kurtosis`.
- **Window**: `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `ntile`, and more.
- **Hash**: `md5`, `sha1`, `sha2`, `crc32`.
- **UDF**: `udf` (register Python UDFs).

### IRISPARK-native aggregates

`IRISPARK.CORR`, `IRISPARK.SKEWNESS`, `IRISPARK.KURTOSIS`, `IRISPARK.MEDIAN`,
`IRISPARK.PERCENTILE`, `IRISPARK.QUANTILE`, `IRISPARK.AGG_MAX_BY`, `IRISPARK.AGG_MIN_BY`,
`IRISPARK.AGG_FIRST`, `IRISPARK.AGG_LAST`.

---

## Types (`irispark.types`)

`IntegerType`, `LongType`, `FloatType`, `DoubleType`, `StringType`, `BooleanType`,
`DateType`, `TimestampType`, `DecimalType`, `ArrayType`, `MapType`, `StructType`,
`StructField`, and more.

---

## Window

```python
from irispark import Window
from irispark.functions import row_number, rank, lag, lead, col

w = Window.partitionBy("department").orderBy(col("salary").desc())
df.withColumn("rn", row_number().over(w)) \
  .withColumn("prev_salary", lag(col("salary"), 1).over(w))
```

---

## RDD

`parallelize`, `map`, `filter`, `flatMap`, `reduce`, `collect`, `toDF`.

---

## ML Transformers (`irispark.ml`)

`VectorAssembler`, `StringIndexer`, `OneHotEncoder`, `StandardScaler`,
`QuantileDiscretizer`.

---

## I/O (`irispark.io`)

`read_csv`, `read_parquet`, `read_json`, `read_table`, `read_sql`, `read_sql_query`,
`read_sql_table`, `from_pandas`. Each accepts an explicit `session=` parameter (IrisPark
sessions are transient).
