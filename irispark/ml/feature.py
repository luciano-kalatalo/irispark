from __future__ import annotations

import warnings

from irispark.dataframe import IrisDataFrame
from irispark.functions import expr, lit
from irispark.ml.base import Estimator, Model, Transformer
from irispark.ml.linalg import LogicalVector
from irispark.ml.param import TypeConverters
from irispark.sql_generator import SQLGenerator


def _pipeline_sql(df: IrisDataFrame) -> str:
    return SQLGenerator(df).generate()


_NUMERIC_TYPES = {
    "integer", "bigint", "smallint", "tinyint",
    "float", "double", "decimal", "numeric",
}


def _require_numeric_index(df: IrisDataFrame, inputCol: str, who: str) -> None:
    """Raise if ``inputCol`` is not a numeric index column.

    OneHotEncoder expects a numeric index (typically the output of
    StringIndexer). Feeding a raw string column silently compares it against
    integer literals and yields all-zeros — a loud error is far better.
    An empty DataFrame is allowed through: there is no data to mis-encode.
    """
    if df.count() == 0:
        return
    dtype = dict(df.dtypes).get(inputCol)
    if dtype is not None and dtype not in _NUMERIC_TYPES:
        raise ValueError(
            f"{who}: inputCol {inputCol!r} has type {dtype!r}; OneHotEncoder "
            f"requires a numeric index column (e.g. the output of "
            f"StringIndexer). Chain StringIndexer first, or pass a numeric "
            f"column."
        )


# ─────────────────────────── QuantileDiscretizer ───────────────────────────


class QuantileDiscretizerModel(Model):
    def __init__(
        self, inputCol: str, outputCol: str, boundaries: list[float]
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self.boundaries = boundaries

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        if not self.boundaries:
            return df.withColumn(self.outputCol, lit(0))

        cases = []
        for i, b in enumerate(self.boundaries):
            cases.append(f"WHEN {self.inputCol} < {b} THEN {i}")

        final_bucket = len(self.boundaries)
        case_expr = f"CASE {' '.join(cases)} ELSE {final_bucket} END"
        return df.withColumn(self.outputCol, expr(case_expr))


class QuantileDiscretizer(Estimator):
    def __init__(
        self,
        numBuckets: int,
        inputCol: str,
        outputCol: str,
    ) -> None:
        super().__init__()
        self._register("numBuckets", "number of buckets", numBuckets, TypeConverters.toInt)
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)

    def getNumBuckets(self) -> int:
        return self.getOrDefault("numBuckets")

    def setNumBuckets(self, value: int) -> QuantileDiscretizer:
        return self._set("numBuckets", value)

    def getInputCol(self) -> str:
        return self.getOrDefault("inputCol")

    def setInputCol(self, value: str) -> QuantileDiscretizer:
        return self._set("inputCol", value)

    def getOutputCol(self) -> str:
        return self.getOrDefault("outputCol")

    def setOutputCol(self, value: str) -> QuantileDiscretizer:
        return self._set("outputCol", value)

    def _fit(self, df: IrisDataFrame) -> QuantileDiscretizerModel:
        if self.numBuckets < 2:
            return QuantileDiscretizerModel(
                inputCol=self.inputCol, outputCol=self.outputCol, boundaries=[]
            )

        probabilities = [i / self.numBuckets for i in range(1, self.numBuckets)]
        boundaries = df.approxQuantile(self.inputCol, probabilities, 0.01)
        return QuantileDiscretizerModel(
            inputCol=self.inputCol, outputCol=self.outputCol, boundaries=boundaries
        )


# ───────────────────────────── VectorAssembler ─────────────────────────────


class VectorAssembler(Transformer):
    def __init__(
        self,
        inputCols: list[str],
        outputCol: str,
        handleInvalid: str = "error",
    ) -> None:
        super().__init__()
        self._register("inputCols", "input columns", inputCols, TypeConverters.toListString)
        self._register("outputCol", "output column", outputCol)
        self._register("handleInvalid", "how to handle invalid values", handleInvalid)

    def getInputCols(self) -> list[str]:
        return self.getOrDefault("inputCols")

    def setInputCols(self, value: list[str]) -> VectorAssembler:
        return self._set("inputCols", value)

    def getOutputCol(self) -> str:
        return self.getOrDefault("outputCol")

    def setOutputCol(self, value: str) -> VectorAssembler:
        return self._set("outputCol", value)

    @property
    def logicalVector(self) -> LogicalVector:
        """Metadata-only feature vector (no materialization, per ml_scope §9)."""
        return LogicalVector(columns=self.inputCols, vectorType="dense")

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        existing = set(df.columns)
        for c in self.inputCols:
            if c not in existing:
                raise ValueError(f"Column {c!r} does not exist in DataFrame")
        # IRIS has no native vector type, so the assembled features are emitted
        # as a comma-joined string column (e.g. "31.0,10.0"). ML estimators
        # consume the raw numeric featuresCol columns directly, so this is
        # illustrative only — warn so callers don't expect a numeric vector.
        warnings.warn(
            "VectorAssembler emits a comma-joined string column, not a numeric "
            "vector (IRIS has no native vector type). ML estimators consume the "
            "raw featuresCol columns directly.",
            UserWarning,
            stacklevel=2,
        )
        parts = []
        for c in self.inputCols:
            parts.append(f"COALESCE(CAST({c} AS VARCHAR),'NaN')")
        concat_expr = " || ',' || ".join(parts)
        return df.withColumn(self.outputCol, expr(concat_expr))


# ───────────────────────────── StringIndexer ──────────────────────────────


class StringIndexerModel(Model):
    def __init__(
        self,
        inputCol: str,
        outputCol: str,
        handleInvalid: str = "error",
        labels: list[str] | None = None,
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self._register("handleInvalid", "how to handle invalid values", handleInvalid)
        self.labels_ = labels or []

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        if not self.labels_:
            return df.withColumn(self.outputCol, lit(0))

        cases = []
        for i, label in enumerate(self.labels_):
            if label == "__NULL__":
                cases.append(f"WHEN {self.inputCol} IS NULL THEN {i}")
            else:
                escaped = label.replace("'", "''")
                cases.append(f"WHEN {self.inputCol} = '{escaped}' THEN {i}")

        if self.handleInvalid == "keep":
            else_clause = f" ELSE {len(self.labels_)}"
        else:
            else_clause = " ELSE NULL"

        case_expr = f"CASE {' '.join(cases)}{else_clause} END"
        return df.withColumn(self.outputCol, expr(case_expr))


class StringIndexer(Estimator):
    def __init__(
        self,
        inputCol: str,
        outputCol: str,
        handleInvalid: str = "error",
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self._register("handleInvalid", "how to handle invalid values", handleInvalid)

    def getInputCol(self) -> str:
        return self.getOrDefault("inputCol")

    def setInputCol(self, value: str) -> StringIndexer:
        return self._set("inputCol", value)

    def getOutputCol(self) -> str:
        return self.getOrDefault("outputCol")

    def setOutputCol(self, value: str) -> StringIndexer:
        return self._set("outputCol", value)

    def _fit(self, df: IrisDataFrame) -> StringIndexerModel:
        pipeline = _pipeline_sql(df)
        sql = (
            f"SELECT DISTINCT {self.inputCol} FROM ({pipeline}) AS _si "
            f"ORDER BY {self.inputCol}"
        )
        rows, _ = df.session.sql(sql)
        labels = [str(r[0]) if r[0] is not None else "__NULL__" for r in rows]
        return StringIndexerModel(
            inputCol=self.inputCol,
            outputCol=self.outputCol,
            handleInvalid=self.handleInvalid,
            labels=labels,
        )


# ──────────────────────────── OneHotEncoder ───────────────────────────────


class OneHotEncoderModel(Model):
    def __init__(
        self,
        inputCol: str,
        outputCol: str,
        dropLast: bool = True,
        num_categories: int = 0,
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self._register("dropLast", "drop last category", dropLast, TypeConverters.toBoolean)
        self._num_categories = num_categories

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        _require_numeric_index(df, self.inputCol, "OneHotEncoder.transform")
        k = self._num_categories
        if k == 0:
            return df.withColumn(self.outputCol, lit(""))

        n = k - 1 if self.dropLast else k
        if n == 0:
            return df.withColumn(self.outputCol, lit(""))

        parts = []
        for i in range(n):
            parts.append(f"CASE WHEN {self.inputCol} = {i} THEN '1' ELSE '0' END")

        concat_expr = " || ',' || ".join(parts)
        return df.withColumn(self.outputCol, expr(concat_expr))


class OneHotEncoder(Estimator):
    def __init__(
        self,
        inputCol: str,
        outputCol: str,
        dropLast: bool = True,
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self._register("dropLast", "drop last category", dropLast, TypeConverters.toBoolean)

    def getInputCol(self) -> str:
        return self.getOrDefault("inputCol")

    def setInputCol(self, value: str) -> OneHotEncoder:
        return self._set("inputCol", value)

    def getOutputCol(self) -> str:
        return self.getOrDefault("outputCol")

    def setOutputCol(self, value: str) -> OneHotEncoder:
        return self._set("outputCol", value)

    def _fit(self, df: IrisDataFrame) -> OneHotEncoderModel:
        _require_numeric_index(df, self.inputCol, "OneHotEncoder.fit")
        pipeline = _pipeline_sql(df)
        sql = f"SELECT COUNT(DISTINCT {self.inputCol}) FROM ({pipeline}) AS _ohe"
        rows, _ = df.session.sql(sql)
        num_categories = rows[0][0] if rows else 0
        return OneHotEncoderModel(
            inputCol=self.inputCol,
            outputCol=self.outputCol,
            dropLast=self.dropLast,
            num_categories=num_categories,
        )


# ─────────────────────────── StandardScaler ───────────────────────────────


class StandardScalerModel(Model):
    def __init__(
        self,
        inputCol: str,
        outputCol: str,
        withStd: bool = True,
        withMean: bool = False,
        mean: float = 0.0,
        std: float = 1.0,
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self._register("withStd", "scale to unit std", withStd, TypeConverters.toBoolean)
        self._register("withMean", "center with mean", withMean, TypeConverters.toBoolean)
        self.mean_ = mean
        self.std_ = std

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        if self.withMean and self.withStd and self.std_ != 0:
            scale_expr = f"({self.inputCol} - {self.mean_}) / {self.std_}"
        elif self.withMean:
            scale_expr = f"({self.inputCol} - {self.mean_})"
        elif self.withStd and self.std_ != 0:
            scale_expr = f"{self.inputCol} / {self.std_}"
        else:
            scale_expr = self.inputCol
        return df.withColumn(self.outputCol, expr(scale_expr))


class StandardScaler(Estimator):
    def __init__(
        self,
        inputCol: str,
        outputCol: str,
        withStd: bool = True,
        withMean: bool = False,
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self._register("withStd", "scale to unit std", withStd, TypeConverters.toBoolean)
        self._register("withMean", "center with mean", withMean, TypeConverters.toBoolean)

    def getInputCol(self) -> str:
        return self.getOrDefault("inputCol")

    def setInputCol(self, value: str) -> StandardScaler:
        return self._set("inputCol", value)

    def getOutputCol(self) -> str:
        return self.getOrDefault("outputCol")

    def setOutputCol(self, value: str) -> StandardScaler:
        return self._set("outputCol", value)

    def _fit(self, df: IrisDataFrame) -> StandardScalerModel:
        pipeline = _pipeline_sql(df)
        sql = (
            f"SELECT AVG({self.inputCol}), STDDEV({self.inputCol}) "
            f"FROM ({pipeline}) AS _s"
        )
        rows, _ = df.session.sql(sql)
        mean = 0.0
        std = 1.0
        if rows and rows[0][0] is not None:
            mean = float(rows[0][0])
        if rows and rows[0][1] is not None:
            std = float(rows[0][1])
        return StandardScalerModel(
            inputCol=self.inputCol,
            outputCol=self.outputCol,
            withStd=self.withStd,
            withMean=self.withMean,
            mean=mean,
            std=std,
        )


# ─────────────────────────────── Imputer ────────────────────────────────


class ImputerModel(Model):
    def __init__(
        self,
        inputCol: str,
        outputCol: str,
        strategy: str,
        replacement: float,
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self._register("strategy", "imputation strategy", strategy)
        self.replacement = replacement

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        return df.withColumn(
            self.outputCol,
            expr(f"COALESCE({self.inputCol}, {self.replacement})"),
        )


class Imputer(Estimator):
    """Impute missing values with mean, median, or mode (ml_scope §10.5)."""

    def __init__(
        self,
        inputCol: str,
        outputCol: str,
        strategy: str = "mean",
    ) -> None:
        super().__init__()
        if strategy not in ("mean", "median", "mode"):
            raise ValueError(
                f"strategy must be 'mean', 'median' or 'mode', got {strategy!r}"
            )
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self._register("strategy", "imputation strategy", strategy)

    def getInputCol(self) -> str:
        return self.getOrDefault("inputCol")

    def setInputCol(self, value: str) -> Imputer:
        return self._set("inputCol", value)

    def getOutputCol(self) -> str:
        return self.getOrDefault("outputCol")

    def setOutputCol(self, value: str) -> Imputer:
        return self._set("outputCol", value)

    def _fit(self, df: IrisDataFrame) -> ImputerModel:
        pipeline = _pipeline_sql(df)
        if self.strategy == "mean":
            sql = f"SELECT AVG({self.inputCol}) FROM ({pipeline}) AS _im"
        elif self.strategy == "median":
            sql = f"SELECT IRISPARK.MEDIAN({self.inputCol}) FROM ({pipeline}) AS _im"
        else:  # mode
            sql = (
                f"SELECT {self.inputCol} FROM ({pipeline}) AS _im "
                f"WHERE {self.inputCol} IS NOT NULL "
                f"GROUP BY {self.inputCol} ORDER BY COUNT(*) DESC, {self.inputCol} "
                f"LIMIT 1"
            )
        rows, _ = df.session.sql(sql)
        replacement = float(rows[0][0]) if rows and rows[0][0] is not None else 0.0
        return ImputerModel(
            inputCol=self.inputCol,
            outputCol=self.outputCol,
            strategy=self.strategy,
            replacement=replacement,
        )


# ─────────────────────────────── Binarizer ───────────────────────────────


class Binarizer(Transformer):
    """Binarize a numeric column by threshold (ml_scope §10.1)."""

    def __init__(
        self,
        inputCol: str,
        outputCol: str,
        threshold: float = 0.0,
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self._register("threshold", "binarization threshold", threshold, TypeConverters.toFloat)

    def getInputCol(self) -> str:
        return self.getOrDefault("inputCol")

    def setInputCol(self, value: str) -> Binarizer:
        return self._set("inputCol", value)

    def getOutputCol(self) -> str:
        return self.getOrDefault("outputCol")

    def setOutputCol(self, value: str) -> Binarizer:
        return self._set("outputCol", value)

    def getThreshold(self) -> float:
        return self.getOrDefault("threshold")

    def setThreshold(self, value: float) -> Binarizer:
        return self._set("threshold", value)

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        return df.withColumn(
            self.outputCol,
            expr(f"CASE WHEN {self.inputCol} > {self.threshold} THEN 1.0 ELSE 0.0 END"),
        )


# ────────────────────────────── MinMaxScaler ──────────────────────────────


class MinMaxScalerModel(Model):
    def __init__(
        self,
        inputCol: str,
        outputCol: str,
        min_: float,
        max_: float,
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self.min_ = min_
        self.max_ = max_

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        if self.max_ == self.min_:
            scale_expr = "0.0"
        else:
            scale_expr = f"({self.inputCol} - {self.min_}) / ({self.max_} - {self.min_})"
        return df.withColumn(self.outputCol, expr(scale_expr))


class MinMaxScaler(Estimator):
    """Scale a numeric column to [0, 1] (ml_scope §11)."""

    def __init__(
        self,
        inputCol: str,
        outputCol: str,
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)

    def getInputCol(self) -> str:
        return self.getOrDefault("inputCol")

    def setInputCol(self, value: str) -> MinMaxScaler:
        return self._set("inputCol", value)

    def getOutputCol(self) -> str:
        return self.getOrDefault("outputCol")

    def setOutputCol(self, value: str) -> MinMaxScaler:
        return self._set("outputCol", value)

    def _fit(self, df: IrisDataFrame) -> MinMaxScalerModel:
        pipeline = _pipeline_sql(df)
        sql = f"SELECT MIN({self.inputCol}), MAX({self.inputCol}) FROM ({pipeline}) AS _mm"
        rows, _ = df.session.sql(sql)
        min_ = float(rows[0][0]) if rows and rows[0][0] is not None else 0.0
        max_ = float(rows[0][1]) if rows and rows[0][1] is not None else 1.0
        return MinMaxScalerModel(
            inputCol=self.inputCol, outputCol=self.outputCol, min_=min_, max_=max_
        )


# ────────────────────────────── MaxAbsScaler ──────────────────────────────


class MaxAbsScalerModel(Model):
    def __init__(
        self,
        inputCol: str,
        outputCol: str,
        max_abs: float,
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self.max_abs = max_abs

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        if self.max_abs == 0:
            scale_expr = self.inputCol
        else:
            scale_expr = f"{self.inputCol} / {self.max_abs}"
        return df.withColumn(self.outputCol, expr(scale_expr))


class MaxAbsScaler(Estimator):
    """Scale by the maximum absolute value (ml_scope §11)."""

    def __init__(
        self,
        inputCol: str,
        outputCol: str,
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)

    def getInputCol(self) -> str:
        return self.getOrDefault("inputCol")

    def setInputCol(self, value: str) -> MaxAbsScaler:
        return self._set("inputCol", value)

    def getOutputCol(self) -> str:
        return self.getOrDefault("outputCol")

    def setOutputCol(self, value: str) -> MaxAbsScaler:
        return self._set("outputCol", value)

    def _fit(self, df: IrisDataFrame) -> MaxAbsScalerModel:
        pipeline = _pipeline_sql(df)
        sql = f"SELECT MAX(ABS({self.inputCol})) FROM ({pipeline}) AS _ma"
        rows, _ = df.session.sql(sql)
        max_abs = float(rows[0][0]) if rows and rows[0][0] is not None else 0.0
        return MaxAbsScalerModel(
            inputCol=self.inputCol, outputCol=self.outputCol, max_abs=max_abs
        )


# ────────────────────────────── IndexToString ──────────────────────────────


class IndexToString(Transformer):
    """Reverse a StringIndexer mapping (ml_scope §10.4)."""

    def __init__(
        self,
        inputCol: str,
        outputCol: str,
        labels: list[str],
    ) -> None:
        super().__init__()
        self._register("inputCol", "input column", inputCol)
        self._register("outputCol", "output column", outputCol)
        self._register("labels", "labels to map indices back to", labels, TypeConverters.toListString)
        self.labels_ = labels

    def getInputCol(self) -> str:
        return self.getOrDefault("inputCol")

    def setInputCol(self, value: str) -> IndexToString:
        return self._set("inputCol", value)

    def getOutputCol(self) -> str:
        return self.getOrDefault("outputCol")

    def setOutputCol(self, value: str) -> IndexToString:
        return self._set("outputCol", value)

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        cases = []
        for i, label in enumerate(self.labels_):
            escaped = label.replace("'", "''")
            cases.append(f"WHEN {self.inputCol} = {i} THEN '{escaped}'")
        case_expr = f"CASE {' '.join(cases)} ELSE NULL END"
        return df.withColumn(self.outputCol, expr(case_expr))


# ────────────────────────────── SQLTransformer ──────────────────────────────


class SQLTransformer(Transformer):
    """Run a SQL statement over the logical DataFrame (ml_scope §13).

    The statement may reference the input as ``__THIS__``; it is substituted
    with the generated SQL of the input DataFrame.
    """

    def __init__(self, statement: str) -> None:
        super().__init__()
        self._register("statement", "SQL statement with __THIS__ placeholder", statement)

    def getStatement(self) -> str:
        return self.getOrDefault("statement")

    def setStatement(self, value: str) -> SQLTransformer:
        return self._set("statement", value)

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        source = _pipeline_sql(df)
        stmt = self.statement.replace("__THIS__", f"({source}) AS __this__")
        rows, columns = df.session.sql(stmt)
        return _rows_to_df(df, rows, columns)


def _rows_to_df(df: IrisDataFrame, rows, columns) -> IrisDataFrame:
    """Materialize SQLTransformer output into a temp table-backed DataFrame."""
    import uuid

    tbl = f"irispark_sqltrans_{uuid.uuid4().hex[:8]}"
    col_defs = ", ".join(f'"{c}" VARCHAR(4000)' for c in columns)
    df.session.sql(f'CREATE TABLE "{tbl}" ({col_defs})')
    str_rows = [tuple("" if v is None else str(v) for v in row) for row in rows]
    df.session._batch_insert(tbl, list(columns), str_rows)
    out = IrisDataFrame(session=df.session, table_name=tbl)
    out._schema = [(str(c), "VARCHAR(4000)") for c in columns]
    return out


# Export all public classes
__all__ = [
    "VectorAssembler",
    "StringIndexer",
    "StringIndexerModel",
    "OneHotEncoder",
    "OneHotEncoderModel",
    "StandardScaler",
    "StandardScalerModel",
    "QuantileDiscretizer",
    "QuantileDiscretizerModel",
    "Imputer",
    "ImputerModel",
    "Binarizer",
    "MinMaxScaler",
    "MinMaxScalerModel",
    "MaxAbsScaler",
    "MaxAbsScalerModel",
    "IndexToString",
    "SQLTransformer",
]
