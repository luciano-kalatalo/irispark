# PySpark → IrisPark Migration Guide

This guide classifies PySpark code by the amount of change needed to run on IrisPark.

## 1. Works Unchanged

Standard SQL functions with exact semantics. Just change the import:

```python
from irispark.functions import sum, avg, upper, lower, year, month, dayofmonth
```

- `abs`
- `acos`
- `acosh`
- `add_months`
- `any`
- `approx_count_distinct`
- `approx_percentile`
- `asc`
- `ascii`
- `asin`
- `atan`
- `atan2`
- `atanh`
- `avg`
- `bit_length`
- `bool_and`
- `bool_or`
- `broadcast`
- `cast`
- `ceil`
- `ceiling`
- `char_length`
- `charindex`
- `chr`
- `coalesce`
- `col`
- `collect_list`
- `collect_set`
- `column`
- `concat`
- `cos`
- `cosh`
- `count`
- `countDistinct`
- `count_distinct`
- `count_if`
- `covar_pop`
- `covar_samp`
- `crc32`
- `cume_dist`
- `curdate`
- `current_date`
- `current_time`
- `current_timestamp`
- `current_user`
- `date_add`
- `date_diff`
- `date_format`
- `date_from_unix_date`
- `date_part`
- `date_sub`
- `dateadd`
- `datediff`
- `datepart`
- `day`
- `dayname`
- `dayofmonth`
- `dayofweek`
- `dayofyear`
- `degrees`
- `dense_rank`
- `desc`
- `e`
- `elt`
- `endswith`
- `equal_null`
- `every`
- `exp`
- `explode`
- `expm1`
- `expr`
- `extract`
- `find_in_set`
- `first_value`
- `floor`
- `from_unixtime`
- `greatest`
- `hour`
- `ifnull`
- `initcap`
- `instr`
- `isnotnull`
- `isnull`
- `lag`
- `last_day`
- `last_value`
- `lcase`
- `lead`
- `least`
- `left`
- `length`
- `levenshtein`
- `lit`
- `ln`
- `localtimestamp`
- `locate`
- `log`
- `log10`
- `log1p`
- `log2`
- `lower`
- `lpad`
- `ltrim`
- `max`
- `md5`
- `mean`
- `min`
- `minute`
- `month`
- `monthname`
- `months_between`
- `nanvl`
- `negate`
- `negative`
- `next_day`
- `now`
- `nth_value`
- `ntile`
- `nullif`
- `nullifzero`
- `nvl`
- `nvl2`
- `percent_rank`
- `pi`
- `pmod`
- `position`
- `positive`
- `power`
- `quarter`
- `radians`
- `random`
- `rank`
- `regexp_extract`
- `repeat`
- `replace`
- `reverse`
- `right`
- `round`
- `row_number`
- `rpad`
- `rtrim`
- `second`
- `sha1`
- `sha2`
- `sign`
- `signum`
- `sin`
- `sinh`
- `some`
- `soundex`
- `space`
- `sqrt`
- `stack`
- `startswith`
- `std`
- `stddev`
- `stddev_pop`
- `stddev_samp`
- `substr`
- `substring`
- `sum`
- `sumDistinct`
- `sum_distinct`
- `tan`
- `tanh`
- `timestamp_micros`
- `timestamp_millis`
- `timestamp_seconds`
- `timestampdiff`
- `to_date`
- `to_unix_timestamp`
- `trim`
- `trunc`
- `ucase`
- `unix_date`
- `unix_micros`
- `unix_millis`
- `unix_seconds`
- `unix_timestamp`
- `upper`
- `uuid`
- `var`
- `var_pop`
- `var_samp`
- `variance`
- `weekday`
- `weekofyear`
- `when`
- `width_bucket`
- `year`
- `zeroifnull`

## 2. Import Change Only

Functions that work identically but live in a non-standard execution path (Embedded Python UDC or ObjectScript UDAF). No code changes beyond import:

- `conv`
- `format_string`
- `from_utc_timestamp`
- `max_by`
- `median`
- `min_by`
- `parse_url`
- `parse_url_key`
- `percentile`
- `percentile_approx`
- `printf`
- `quantile`
- `regexp_replace`
- `split`
- `to_utc_timestamp`

## 3. Minor Adaptation Required

Functions that are operationally compatible but have documented edge-case deviations:

- `concat_ws`
- `corr`
- `first`
- `kurtosis`
- `last`
- `pow`
- `rand`
- `randn`
- `skewness`
- `uniform`

## 4. IRIS-Specific Adaptation

Features that require understanding IRIS-specific behaviour or are IrisPark extensions:

- `udf`

## 5. Unsupported

Functions or modes that are intentionally not implemented:

_None currently._

---
_Last updated: auto-generated from `irispark/registry.py`._
