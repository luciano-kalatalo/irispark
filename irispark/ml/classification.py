from __future__ import annotations

from typing import Any

import numpy as np

from irispark.dataframe import IrisDataFrame
from irispark.functions import expr
from irispark.ml.base import Estimator, Model
from irispark.ml.linalg import LogicalVector, resolve_features
from irispark.ml.param import TypeConverters


class LogisticRegressionModel(Model):
    """Fitted logistic regression; predicts via SQL pushdown."""

    def __init__(
        self,
        featuresCol: list[str],
        labelCol: str,
        predictionCol: str,
        probabilityCol: str,
        coefficients: list[float],
        intercept: float,
        threshold: float,
        logicalVector: LogicalVector | None = None,
    ) -> None:
        super().__init__()
        self._register("featuresCol", "feature columns", featuresCol, TypeConverters.toListString)
        self._register("labelCol", "label column", labelCol)
        self._register("predictionCol", "prediction column", predictionCol)
        self._register("probabilityCol", "probability column", probabilityCol)
        self._register("threshold", "classification threshold", threshold, TypeConverters.toFloat)
        self.coefficients = list(coefficients)
        self.intercept = float(intercept)
        self.logicalVector = logicalVector

    def _logit_expr(self) -> str:
        terms = [f"{c} * {w}" for c, w in zip(self.featuresCol, self.coefficients)]
        return " + ".join([str(self.intercept)] + terms)

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        logit = self._logit_expr()
        prob_expr = f"1 / (1 + EXP(-({logit})))"
        df = df.withColumn(self.probabilityCol, expr(prob_expr))
        return df.withColumn(
            self.predictionCol,
            expr(f"CASE WHEN {self.probabilityCol} >= {self.threshold} THEN 1.0 ELSE 0.0 END"),
        )

    def evaluate(self, df: IrisDataFrame) -> Any:
        """PySpark-compatible: score ``df`` and return a ``BinaryClassificationSummary``.

        The summary exposes ``.predictions`` plus ``accuracy`` / ``precision`` /
        ``recall`` / ``fMeasureByLabel`` / ``areaUnderROC`` / ``numInstances``.
        """
        from irispark.ml.evaluation import BinaryClassificationSummary

        return BinaryClassificationSummary(
            self, df,
            predictionCol=self.predictionCol,
            labelCol=self.labelCol,
            probabilityCol=self.probabilityCol,
        )


class LogisticRegression(Estimator):
    """Binary logistic regression fit with numpy gradient descent.

    ``fit()`` trains on the numpy feature matrix (L2-regularized); ``transform()``
    pushes the learned coefficients back as a SQL sigmoid, so scoring never
    moves data to Python (ml_scope §38/§39).
    """

    def __init__(
        self,
        featuresCol,
        labelCol: str,
        predictionCol: str = "prediction",
        probabilityCol: str = "probability",
        regParam: float = 0.0,
        maxIter: int = 100,
        threshold: float = 0.5,
        learningRate: float = 0.1,
    ) -> None:
        super().__init__()
        self._origin_vector = (
            featuresCol if isinstance(featuresCol, LogicalVector) else None
        )
        self._register("featuresCol", "feature columns", resolve_features(featuresCol), TypeConverters.toListString)
        self._register("labelCol", "label column", labelCol)
        self._register("predictionCol", "prediction column", predictionCol)
        self._register("probabilityCol", "probability column", probabilityCol)
        self._register("regParam", "L2 regularization", regParam, TypeConverters.toFloat)
        self._register("maxIter", "max iterations", maxIter, TypeConverters.toInt)
        self._register("threshold", "classification threshold", threshold, TypeConverters.toFloat)
        self._register("learningRate", "gradient descent learning rate", learningRate, TypeConverters.toFloat)

    def getFeaturesCol(self) -> list[str]:
        return self.getOrDefault("featuresCol")

    @property
    def logicalVector(self):
        return self._origin_vector

    def setFeaturesCol(self, value: list[str]) -> LogisticRegression:
        return self._set("featuresCol", value)

    def getLabelCol(self) -> str:
        return self.getOrDefault("labelCol")

    def setLabelCol(self, value: str) -> LogisticRegression:
        return self._set("labelCol", value)

    def getPredictionCol(self) -> str:
        return self.getOrDefault("predictionCol")

    def setPredictionCol(self, value: str) -> LogisticRegression:
        return self._set("predictionCol", value)

    def _fit(self, df: IrisDataFrame) -> LogisticRegressionModel:
        cols = self.featuresCol + [self.labelCol]
        pdf = df.select(*cols).to_pandas()
        X = pdf[self.featuresCol].to_numpy(dtype=float)
        y = pdf[self.labelCol].to_numpy(dtype=float)
        Xb = np.column_stack([np.ones(len(X)), X])
        n, d = Xb.shape
        beta = np.zeros(d)
        lam = self.regParam
        lr = self.learningRate
        for _ in range(self.maxIter):
            z = Xb @ beta
            p = 1.0 / (1.0 + np.exp(-np.clip(z, -500, 500)))
            grad = Xb.T @ (p - y) / n
            if lam > 0:
                grad[1:] += lam * beta[1:]
            beta -= lr * grad
        intercept = float(beta[0])
        coefficients = [float(v) for v in beta[1:]]
        return LogisticRegressionModel(
            featuresCol=self.featuresCol,
            labelCol=self.labelCol,
            predictionCol=self.predictionCol,
            probabilityCol=self.probabilityCol,
            coefficients=coefficients,
            intercept=intercept,
            threshold=self.threshold,
            logicalVector=self._origin_vector,
        )
