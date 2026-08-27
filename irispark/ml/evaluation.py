from __future__ import annotations

from typing import Any

from irispark.dataframe import IrisDataFrame
from irispark.ml.param import Params


class RegressionSummary:
    """PySpark-compatible regression evaluation result.

    Wraps a fitted model and a scored DataFrame to expose ``.predictions`` and
    the regression metric accessors (MAE / MSE / RMSE / R²), computed in SQL.
    """

    def __init__(
        self,
        model: Any,
        df: IrisDataFrame,
        predictionCol: str = "prediction",
        labelCol: str = "label",
    ) -> None:
        self._model = model
        self._df = df
        self._predictionCol = predictionCol
        self._labelCol = labelCol
        self._predictions: IrisDataFrame | None = None

    @property
    def predictions(self) -> IrisDataFrame:
        if self._predictions is None:
            self._predictions = self._model.transform(self._df)
        return self._predictions

    def _metric(self, name: str) -> float:
        return RegressionEvaluator(
            predictionCol=self._predictionCol,
            labelCol=self._labelCol,
            metricName=name,
        ).evaluate(self.predictions)

    @property
    def numInstances(self) -> int:
        return self._df.count()

    @property
    def meanAbsoluteError(self) -> float:
        return self._metric("mae")

    @property
    def meanSquaredError(self) -> float:
        return self._metric("mse")

    @property
    def rootMeanSquaredError(self) -> float:
        return self._metric("rmse")

    @property
    def r2(self) -> float:
        return self._metric("r2")


class BinaryClassificationSummary:
    """PySpark-compatible binary classification evaluation result.

    Exposes ``.predictions`` plus accuracy / precision / recall / F1 / AUC,
    computed in SQL (AUC via numpy ranking).
    """

    def __init__(
        self,
        model: Any,
        df: IrisDataFrame,
        predictionCol: str = "prediction",
        labelCol: str = "label",
        probabilityCol: str = "probability",
    ) -> None:
        self._model = model
        self._df = df
        self._predictionCol = predictionCol
        self._labelCol = labelCol
        self._probabilityCol = probabilityCol
        self._predictions: IrisDataFrame | None = None

    @property
    def predictions(self) -> IrisDataFrame:
        if self._predictions is None:
            self._predictions = self._model.transform(self._df)
        return self._predictions

    def _metric(self, name: str) -> float:
        return BinaryClassificationEvaluator(
            predictionCol=self._predictionCol,
            labelCol=self._labelCol,
            probabilityCol=self._probabilityCol,
            metricName=name,
        ).evaluate(self.predictions)

    @property
    def numInstances(self) -> int:
        return self._df.count()

    @property
    def accuracy(self) -> float:
        return self._metric("accuracy")

    @property
    def precision(self) -> float:
        return self._metric("precision")

    @property
    def recall(self) -> float:
        return self._metric("recall")

    @property
    def fMeasureByLabel(self) -> float:
        return self._metric("f1")

    @property
    def areaUnderROC(self) -> float:
        return self._metric("areaUnderROC")



class RegressionEvaluator(Params):
    """Regression metrics computed in SQL (ml_scope §22)."""

    def __init__(
        self,
        predictionCol: str = "prediction",
        labelCol: str = "label",
        metricName: str = "rmse",
    ) -> None:
        super().__init__()
        self._register("predictionCol", "prediction column", predictionCol)
        self._register("labelCol", "label column", labelCol)
        self._register("metricName", "metric name", metricName)

    def getMetricName(self) -> str:
        return self.getOrDefault("metricName")

    def setMetricName(self, value: str) -> RegressionEvaluator:
        return self._set("metricName", value)

    def evaluate(self, df: IrisDataFrame) -> float:
        pred_col, label_col = self.predictionCol, self.labelCol
        metric = self.metricName.lower()
        if metric == "mae":
            sql = f"SELECT AVG(ABS({pred_col} - {label_col})) FROM ({df.to_sql()}) AS _ev"
        elif metric == "mse":
            sql = f"SELECT AVG(({pred_col} - {label_col}) * ({pred_col} - {label_col})) FROM ({df.to_sql()}) AS _ev"
        elif metric == "rmse":
            sql = f"SELECT SQRT(AVG(({pred_col} - {label_col}) * ({pred_col} - {label_col}))) FROM ({df.to_sql()}) AS _ev"
        elif metric in ("r2", "r2"):
            # R² = 1 - SS_res / SS_tot. Compute the mean label in a subquery
            # (a window function cannot be nested inside an aggregate in IRIS).
            sql = (
                f"SELECT 1 - (SUM(({pred_col} - {label_col}) * ({pred_col} - {label_col})) / "
                f"SUM(({label_col} - m) * ({label_col} - m))) FROM ("
                f"SELECT *, AVG({label_col}) OVER () AS m FROM ({df.to_sql()}) AS _ev"
                f") AS _ev2"
            )
        else:
            raise ValueError(f"Unknown metric {self.metricName!r}")
        rows, _ = df.session.sql(sql)
        return float(rows[0][0]) if rows and rows[0][0] is not None else float("nan")


class BinaryClassificationEvaluator(Params):
    """Binary classification metrics computed in SQL (ml_scope §23)."""

    def __init__(
        self,
        predictionCol: str = "prediction",
        labelCol: str = "label",
        probabilityCol: str = "probability",
        metricName: str = "areaUnderROC",
    ) -> None:
        super().__init__()
        self._register("predictionCol", "prediction column", predictionCol)
        self._register("labelCol", "label column", labelCol)
        self._register("probabilityCol", "probability column", probabilityCol)
        self._register("metricName", "metric name", metricName)

    def getMetricName(self) -> str:
        return self.getOrDefault("metricName")

    def setMetricName(self, value: str) -> BinaryClassificationEvaluator:
        return self._set("metricName", value)

    def evaluate(self, df: IrisDataFrame) -> float:
        pred_col, label_col = self.predictionCol, self.labelCol
        metric = self.metricName.lower()
        src = f"({df.to_sql()}) AS _ev"
        if metric in ("accuracy", "accuracy"):
            sql = f"SELECT AVG(CASE WHEN {pred_col} = {label_col} THEN 1.0 ELSE 0.0 END) FROM {src}"
        elif metric in ("precision", "precision"):
            sql = (
                f"SELECT SUM(CASE WHEN {pred_col} = 1 AND {label_col} = 1 THEN 1 ELSE 0 END) / "
                f"NULLIF(SUM(CASE WHEN {pred_col} = 1 THEN 1 ELSE 0 END), 0) FROM {src}"
            )
        elif metric in ("recall", "recall"):
            sql = (
                f"SELECT SUM(CASE WHEN {pred_col} = 1 AND {label_col} = 1 THEN 1 ELSE 0 END) / "
                f"NULLIF(SUM(CASE WHEN {label_col} = 1 THEN 1 ELSE 0 END), 0) FROM {src}"
            )
        elif metric in ("f1", "f1"):
            sql = (
                f"SELECT 2 * (SUM(CASE WHEN {pred_col} = 1 AND {label_col} = 1 THEN 1 ELSE 0 END)) / "
                f"NULLIF(SUM(CASE WHEN {pred_col} = 1 THEN 1 ELSE 0 END) + "
                f"SUM(CASE WHEN {label_col} = 1 THEN 1 ELSE 0 END), 0) FROM {src}"
            )
        elif metric in ("areaunderroc", "areaunderroc"):
            # Ranking metric: compute AUC in Python from the two columns
            # (spec §23 allows Python for ranking-based metrics).
            import numpy as np

            pdf = df.select(self.probabilityCol, self.labelCol).to_pandas()
            prob = pdf[self.probabilityCol].to_numpy(dtype=float)
            label = pdf[self.labelCol].to_numpy(dtype=float)
            pos = prob[label == 1]
            neg = prob[label == 0]
            if len(pos) == 0 or len(neg) == 0:
                return float("nan")
            # Mann-Whitney U / (n_pos * n_neg)
            auc = (np.sum(pos[:, None] > neg[None, :]) + 0.5 * np.sum(pos[:, None] == neg[None, :])) / (len(pos) * len(neg))
            return float(auc)
        else:
            raise ValueError(f"Unknown metric {self.metricName!r}")
        rows, _ = df.session.sql(sql)
        return float(rows[0][0]) if rows and rows[0][0] is not None else float("nan")
