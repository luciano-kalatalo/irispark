# IrisPark Compatibility Matrix

This document lists every IrisPark SQL function, its PySpark mapping, execution engine, and compatibility level.

## Compatibility Levels

| Level | Meaning |
| ----- | ------- |
| **A** | Exact target compatibility (tested against PySpark) |
| **B** | Operational compatibility (common cases equivalent, documented edge-case deviations) |
| **C** | Syntax compatibility (API shape preserved, semantics differ) |
| **D** | Partial support (specific modes or parameters only) |
| **E** | Unsupported (intentionally not implemented) |

## Matrix

| IrisPark Name | PySpark Name | Category | Status | Compatibility | Execution | Notes |
| ------------- | ------------ | -------- | ------ | ------------- | --------- | ----- |
| `approx_count_distinct` | `approx_count_distinct` | aggregate | Supported | A | sql_composition | Exact COUNT(DISTINCT) — no HyperLogLog approximation yet |
| `approx_percentile` | `approx_percentile` | aggregate | Supported | A | sql_composition | Alias of percentile_approx |
| `avg` | `avg` | aggregate | Supported | A | native_sql | — |
| `bool_and` | `bool_and` | aggregate | Supported | A | sql_composition | MIN(CASE WHEN ... THEN 1 ELSE 0) |
| `bool_or` | `bool_or` | aggregate | Supported | A | sql_composition | MAX(CASE WHEN ... THEN 1 ELSE 0) |
| `count` | `count` | aggregate | Supported | A | native_sql | — |
| `countDistinct` | `countDistinct` | aggregate | Supported | A | sql_composition | Multi-col form concatenates with chr(0) |
| `count_distinct` | `countDistinct` | aggregate | Supported | A | native_sql | Snake-case alias |
| `count_if` | `count_if` | aggregate | Supported | A | sql_composition | SUM(CASE WHEN ... THEN 1 ELSE 0) |
| `covar_pop` | `covar_pop` | aggregate | Supported | A | sql_composition | Pairwise-complete (null-excluding); matched against PySpark |
| `covar_samp` | `covar_samp` | aggregate | Supported | A | sql_composition | Pairwise-complete (null-excluding); matched against PySpark |
| `first` | `first` | aggregate | Supported | B | objectscript | IRISPARK.AGG_FIRST UDAF; documented ignorenulls deviation |
| `kurtosis` | `kurtosis` | aggregate | Supported | B | objectscript | IRISPARK.KURTOSIS UDAF (ObjectScript) |
| `last` | `last` | aggregate | Supported | B | objectscript | IRISPARK.AGG_LAST UDAF; documented ignorenulls deviation |
| `max` | `max` | aggregate | Supported | A | native_sql | — |
| `max_by` | `max_by` | aggregate | Supported | A | objectscript | IRISPARK.AGG_MAX_BY UDAF |
| `mean` | `mean` | aggregate | Supported | A | native_sql | Alias of AVG |
| `median` | `median` | aggregate | Supported | A | objectscript | IRISPARK_MEDIAN_ANALYTIC (analytic engine) |
| `min` | `min` | aggregate | Supported | A | native_sql | — |
| `min_by` | `min_by` | aggregate | Supported | A | objectscript | IRISPARK.AGG_MIN_BY UDAF |
| `percentile` | `percentile` | aggregate | Supported | A | objectscript | IRISPARK_PERCENTILE_ANALYTIC (analytic engine) |
| `percentile_approx` | `percentile_approx` | aggregate | Supported | A | objectscript | IRISPARK_PERCENTILE_ANALYTIC (analytic engine) |
| `quantile` | `quantile` | aggregate | Supported | A | objectscript | IRISPARK_QUANTILE_ANALYTIC (analytic engine) |
| `skewness` | `skewness` | aggregate | Supported | B | objectscript | IRISPARK.SKEWNESS UDAF (ObjectScript) |
| `std` | `stddev` | aggregate | Supported | A | native_sql | Alias of stddev |
| `stddev` | `stddev` | aggregate | Supported | A | native_sql | — |
| `stddev_pop` | `stddev_pop` | aggregate | Supported | A | native_sql | — |
| `stddev_samp` | `stddev_samp` | aggregate | Supported | A | native_sql | — |
| `sum` | `sum` | aggregate | Supported | A | native_sql | — |
| `sumDistinct` | `sumDistinct` | aggregate | Supported | A | native_sql | SUM(DISTINCT) |
| `sum_distinct` | `sumDistinct` | aggregate | Supported | A | native_sql | Snake-case alias |
| `var` | `var_samp` | aggregate | Supported | A | native_sql | Alias of var_samp |
| `var_pop` | `var_pop` | aggregate | Supported | A | native_sql | — |
| `var_samp` | `var_samp` | aggregate | Supported | A | native_sql | — |
| `variance` | `var_samp` | aggregate | Supported | A | native_sql | Alias of var_samp |
| `any` | `bool_or` | conditional | Supported | A | native_sql | Alias of bool_or |
| `coalesce` | `coalesce` | conditional | Supported | A | native_sql | — |
| `equal_null` | `equal_null` | conditional | Supported | A | sql_composition | NULL-safe equality |
| `every` | `bool_and` | conditional | Supported | A | native_sql | Alias of bool_and |
| `ifnull` | `ifnull` | conditional | Supported | A | sql_composition | Renders as COALESCE (IRIS IFNULL has quirk) |
| `isnotnull` | `isnotnull` | conditional | Supported | A | sql_composition | PredicateColumn dual form |
| `isnull` | `isnull` | conditional | Supported | A | sql_composition | PredicateColumn dual form |
| `nanvl` | `nanvl` | conditional | Supported | A | sql_composition | IRIS has no NaN; identity for non-NULL |
| `nullif` | `nullif` | conditional | Supported | A | native_sql | — |
| `nullifzero` | `nullifzero` | conditional | Supported | A | native_sql | — |
| `nvl` | `nvl` | conditional | Supported | A | sql_composition | Alias of ifnull; renders as COALESCE |
| `nvl2` | `nvl2` | conditional | Supported | A | sql_composition | — |
| `some` | `bool_or` | conditional | Supported | A | native_sql | Alias of bool_or |
| `when` | `when` | conditional | Supported | A | sql_composition | CASE WHEN chain |
| `zeroifnull` | `zeroifnull` | conditional | Supported | A | sql_composition | — |
| `add_months` | `add_months` | datetime | Supported | A | sql_composition | — |
| `curdate` | `current_date` | datetime | Supported | A | native_sql | Alias of current_date |
| `current_date` | `current_date` | datetime | Supported | A | native_sql | — |
| `current_time` | `current_time` | datetime | Supported | A | native_sql | — |
| `current_timestamp` | `current_timestamp` | datetime | Supported | A | native_sql | — |
| `date_add` | `date_add` | datetime | Supported | A | native_sql | — |
| `date_diff` | `datediff` | datetime | Supported | A | native_sql | Alias of datediff |
| `date_format` | `date_format` | datetime | Supported | A | native_sql | — |
| `date_from_unix_date` | `date_from_unix_date` | datetime | Supported | A | sql_composition | — |
| `date_part` | `date_part` | datetime | Supported | A | native_sql | Alias of extract |
| `date_sub` | `date_sub` | datetime | Supported | A | native_sql | — |
| `dateadd` | `date_add` | datetime | Supported | A | native_sql | Alias of date_add |
| `datediff` | `datediff` | datetime | Supported | A | native_sql | — |
| `datepart` | `date_part` | datetime | Supported | A | native_sql | Alias of date_part / extract |
| `day` | `dayofmonth` | datetime | Supported | A | native_sql | Alias of dayofmonth |
| `dayname` | `dayname` | datetime | Supported | A | native_sql | — |
| `dayofmonth` | `dayofmonth` | datetime | Supported | A | native_sql | — |
| `dayofweek` | `dayofweek` | datetime | Supported | A | native_sql | — |
| `dayofyear` | `dayofyear` | datetime | Supported | A | native_sql | — |
| `extract` | `extract` | datetime | Supported | A | native_sql | — |
| `from_unixtime` | `from_unixtime` | datetime | Supported | A | sql_composition | — |
| `hour` | `hour` | datetime | Supported | A | native_sql | — |
| `last_day` | `last_day` | datetime | Supported | A | native_sql | — |
| `localtimestamp` | `localtimestamp` | datetime | Supported | A | sql_composition | Alias of CURRENT_TIMESTAMP (IRIS timestamps are local) |
| `minute` | `minute` | datetime | Supported | A | native_sql | — |
| `month` | `month` | datetime | Supported | A | native_sql | — |
| `monthname` | `monthname` | datetime | Supported | A | native_sql | — |
| `months_between` | `months_between` | datetime | Supported | A | sql_composition | Matched against live PySpark 3.5.8; both-last-day exact rule applied |
| `next_day` | `next_day` | datetime | Supported | A | sql_composition | — |
| `now` | `current_timestamp` | datetime | Supported | A | native_sql | Alias of current_timestamp |
| `quarter` | `quarter` | datetime | Supported | A | native_sql | — |
| `second` | `second` | datetime | Supported | A | native_sql | — |
| `timestamp_micros` | `timestamp_micros` | datetime | Supported | A | sql_composition | — |
| `timestamp_millis` | `timestamp_millis` | datetime | Supported | A | sql_composition | — |
| `timestamp_seconds` | `timestamp_seconds` | datetime | Supported | A | sql_composition | — |
| `timestampdiff` | `timestampdiff` | datetime | Supported | A | sql_composition | Matched against live PySpark 3.5.8 on month-end/leap vectors |
| `to_date` | `to_date` | datetime | Supported | A | native_sql | — |
| `to_unix_timestamp` | `to_unix_timestamp` | datetime | Supported | A | sql_composition | — |
| `trunc` | `trunc` | datetime | Supported | A | sql_composition | year/month/quarter/week only |
| `unix_date` | `unix_date` | datetime | Supported | A | sql_composition | — |
| `unix_micros` | `unix_micros` | datetime | Supported | A | sql_composition | — |
| `unix_millis` | `unix_millis` | datetime | Supported | A | sql_composition | — |
| `unix_seconds` | `unix_seconds` | datetime | Supported | A | sql_composition | — |
| `unix_timestamp` | `unix_timestamp` | datetime | Supported | A | sql_composition | — |
| `weekday` | `weekday` | datetime | Supported | A | sql_composition | 0=Monday..6=Sunday (IRIS DAYOFWEEK is 1=Sunday..) |
| `weekofyear` | `weekofyear` | datetime | Supported | A | native_sql | — |
| `year` | `year` | datetime | Supported | A | native_sql | — |
| `abs` | `abs` | math | Supported | A | native_sql | — |
| `acos` | `acos` | math | Supported | A | sql_composition | Guarded against domain errors (IRIS ACOS fatals out-of-domain) |
| `acosh` | `acosh` | math | Supported | A | sql_composition | Log formulation |
| `asin` | `asin` | math | Supported | A | sql_composition | Guarded against domain errors (IRIS ASIN fatals out-of-domain) |
| `atan` | `atan` | math | Supported | A | native_sql | — |
| `atan2` | `atan2` | math | Supported | A | sql_composition | NULL guards (IRIS ATAN2 fatals on NULL) |
| `atanh` | `atanh` | math | Supported | A | sql_composition | — |
| `ceil` | `ceil` | math | Supported | A | native_sql | IRIS CEILING |
| `ceiling` | `ceil` | math | Supported | A | native_sql | Alias of ceil |
| `cos` | `cos` | math | Supported | A | native_sql | — |
| `cosh` | `cosh` | math | Supported | A | sql_composition | — |
| `degrees` | `degrees` | math | Supported | A | native_sql | — |
| `e` | `e` | math | Supported | A | sql_composition | Constant column EXP(1) |
| `exp` | `exp` | math | Supported | A | native_sql | — |
| `expm1` | `expm1` | math | Supported | A | sql_composition | EXP(x) - 1 |
| `floor` | `floor` | math | Supported | A | native_sql | — |
| `ln` | `ln` | math | Supported | A | native_sql | — |
| `log` | `log` | math | Supported | A | sql_composition | Two-arg form uses change-of-base formula |
| `log10` | `log10` | math | Supported | A | native_sql | — |
| `log1p` | `log1p` | math | Supported | A | sql_composition | — |
| `log2` | `log2` | math | Supported | A | sql_composition | — |
| `negate` | `negative` | math | Supported | A | native_sql | Alias of negative |
| `negative` | `negative` | math | Supported | A | native_sql | — |
| `pi` | `pi` | math | Supported | A | sql_composition | Constant 3.141592653589793 |
| `pmod` | `pmod` | math | Supported | A | sql_composition | Matches Spark pmod exactly (verified) |
| `positive` | `positive` | math | Supported | A | native_sql | — |
| `pow` | `pow` | math | Supported | B | sql_composition | Guards POWER(0,negative) and POWER(negative,fractional) to NULL (IRIS cannot represent Inf/NaN) |
| `power` | `pow` | math | Supported | A | native_sql | Alias of pow |
| `radians` | `radians` | math | Supported | A | native_sql | — |
| `rand` | `rand` | math | Supported | B | sql_composition | Uses irispark_rand ObjectScript helper; non-deterministic without seed |
| `randn` | `randn` | math | Supported | B | sql_composition | Linear transform of rand; not true normal distribution |
| `random` | `rand` | math | Supported | A | native_sql | Alias of rand |
| `round` | `round` | math | Supported | A | native_sql | — |
| `sign` | `sign` | math | Supported | A | native_sql | — |
| `signum` | `sign` | math | Supported | A | native_sql | Alias of sign |
| `sin` | `sin` | math | Supported | A | native_sql | — |
| `sinh` | `sinh` | math | Supported | A | sql_composition | — |
| `sqrt` | `sqrt` | math | Supported | A | native_sql | — |
| `tan` | `tan` | math | Supported | A | native_sql | — |
| `tanh` | `tanh` | math | Supported | A | sql_composition | — |
| `uniform` | `uniform` | math | Supported | B | sql_composition | Uniform via irispark_rand; not cryptographically secure |
| `asc` | `asc` | misc | Supported | A | native_sql | Ascending sort specifier |
| `broadcast` | `broadcast` | misc | Supported | A | sql_composition | No-op (IRIS has no broadcast hint) |
| `cast` | `cast` | misc | Supported | A | native_sql | — |
| `col` | `col` | misc | Supported | A | native_sql | Column reference constructor |
| `column` | `col` | misc | Supported | A | native_sql | Alias of col |
| `crc32` | `crc32` | misc | Supported | A | native_sql | — |
| `current_user` | `current_user` | misc | Supported | A | native_sql | — |
| `desc` | `desc` | misc | Supported | A | native_sql | Descending sort specifier |
| `explode` | `explode` | misc | Supported | A | native_sql | EXPLODE() lateral operator |
| `expr` | `expr` | misc | Supported | A | native_sql | Raw SQL expression |
| `lit` | `lit` | misc | Supported | A | native_sql | Literal value column |
| `md5` | `md5` | misc | Supported | A | native_sql | — |
| `sha1` | `sha1` | misc | Supported | A | native_sql | — |
| `sha2` | `sha2` | misc | Supported | A | native_sql | — |
| `stack` | `stack` | misc | Supported | A | sql_composition | STACK() lateral operator |
| `udf` | `udf` | misc | Supported | A | python_fallback | @udf decorator registers Python functions as IRIS SQL UDFs |
| `uuid` | `uuid` | misc | Supported | A | sql_composition | IRIS UUID with brace stripping |
| `ascii` | `ascii` | string | Supported | A | native_sql | — |
| `bit_length` | `bit_length` | string | Supported | A | sql_composition | — |
| `char_length` | `char_length` | string | Supported | A | native_sql | — |
| `charindex` | `charindex` | string | Supported | A | sql_composition | CHARINDEX with position-rebased search |
| `chr` | `chr` | string | Supported | A | native_sql | — |
| `collect_list` | `collect_list` | string | Supported | A | native_sql | — |
| `collect_set` | `collect_set` | string | Supported | A | native_sql | — |
| `concat` | `concat` | string | Supported | A | sql_composition | Uses || operator |
| `concat_ws` | `concat_ws` | string | Supported | B | sql_composition | NULL inputs render as empty string (PySpark skips them) |
| `elt` | `elt` | string | Supported | A | sql_composition | — |
| `endswith` | `endswith` | string | Supported | A | sql_composition | %EXACT ... LIKE '%'||suffix |
| `find_in_set` | `find_in_set` | string | Supported | A | sql_composition | — |
| `greatest` | `greatest` | string | Supported | A | native_sql | — |
| `initcap` | `initcap` | string | Supported | A | native_sql | — |
| `instr` | `instr` | string | Supported | A | native_sql | — |
| `lcase` | `lower` | string | Supported | A | native_sql | Alias of lower |
| `least` | `least` | string | Supported | A | native_sql | — |
| `left` | `left` | string | Supported | A | native_sql | — |
| `length` | `length` | string | Supported | A | native_sql | — |
| `levenshtein` | `levenshtein` | string | Supported | A | native_sql | — |
| `locate` | `locate` | string | Supported | A | native_sql | — |
| `lower` | `lower` | string | Supported | A | native_sql | — |
| `lpad` | `lpad` | string | Supported | A | sql_composition | Guards empty pad string (IRIS pads with NUL) |
| `ltrim` | `ltrim` | string | Supported | A | native_sql | — |
| `position` | `position` | string | Supported | A | native_sql | — |
| `regexp_extract` | `regexp_extract` | string | Supported | A | native_sql | — |
| `regexp_replace` | `regexp_replace` | string | Supported | A | embedded_python | irispark_regexp_replace ObjectScript helper |
| `repeat` | `repeat` | string | Supported | A | native_sql | — |
| `replace` | `replace` | string | Supported | A | native_sql | — |
| `reverse` | `reverse` | string | Supported | A | native_sql | — |
| `right` | `right` | string | Supported | A | native_sql | — |
| `rpad` | `rpad` | string | Supported | A | sql_composition | Guards empty pad string (IRIS pads with NUL) |
| `rtrim` | `rtrim` | string | Supported | A | native_sql | — |
| `soundex` | `soundex` | string | Supported | A | native_sql | — |
| `space` | `space` | string | Supported | A | native_sql | — |
| `split` | `split` | string | Supported | A | embedded_python | irispark_split ObjectScript helper |
| `startswith` | `startswith` | string | Supported | A | sql_composition | %EXACT ... LIKE prefix||'%' |
| `substr` | `substring` | string | Supported | A | native_sql | Alias of substring |
| `substring` | `substring` | string | Supported | A | native_sql | — |
| `trim` | `trim` | string | Supported | A | native_sql | — |
| `ucase` | `upper` | string | Supported | A | native_sql | Alias of upper |
| `upper` | `upper` | string | Supported | A | native_sql | — |
| `width_bucket` | `width_bucket` | string | Supported | A | sql_composition | — |
| `corr` | `corr` | udaf | Supported | B | objectscript | IRISPARK.CORR UDAF (Welford online algorithm); SQL composition also available |
| `conv` | `conv` | udc | Supported | A | embedded_python | Pure-Python base conversion; overflow semantics match Spark |
| `format_string` | `format_string` | udc | Supported | A | embedded_python | Python-style formatting via irispark_udc.py |
| `from_utc_timestamp` | `from_utc_timestamp` | udc | Supported | A | embedded_python | zoneinfo via irispark_udc.py |
| `parse_url` | `parse_url` | udc | Supported | A | embedded_python | urllib.parse via irispark_udc.py |
| `parse_url_key` | `parse_url_key` | udc | Supported | A | embedded_python | Query-param extraction via irispark_udc.py |
| `printf` | `printf` | udc | Supported | A | embedded_python | C-style printf via irispark_udc.py |
| `to_utc_timestamp` | `to_utc_timestamp` | udc | Supported | A | embedded_python | zoneinfo via irispark_udc.py |
| `cume_dist` | `cume_dist` | window | Supported | A | native_sql | — |
| `dense_rank` | `dense_rank` | window | Supported | A | native_sql | — |
| `first_value` | `first_value` | window | Supported | A | native_sql | — |
| `lag` | `lag` | window | Supported | A | native_sql | — |
| `last_value` | `last_value` | window | Supported | A | native_sql | — |
| `lead` | `lead` | window | Supported | A | native_sql | — |
| `nth_value` | `nth_value` | window | Supported | A | sql_composition | — |
| `ntile` | `ntile` | window | Supported | A | native_sql | — |
| `percent_rank` | `percent_rank` | window | Supported | A | native_sql | — |
| `rank` | `rank` | window | Supported | A | native_sql | — |
| `row_number` | `row_number` | window | Supported | A | native_sql | — |
