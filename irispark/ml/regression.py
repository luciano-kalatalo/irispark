from __future__ import annotations

from typing import Any

import numpy as np

from irispark.dataframe import IrisDataFrame
from irispark.functions import expr
from irispark.ml.base import Estimator, Model
from irispark.ml.linalg import LogicalVector, resolve_features
from irispark.ml.param import TypeConverters


class LinearRegressionModel(Model):
    """Fitted linear regression; predicts via SQL pushdown."""

    def __init__(
        self,
        featuresCol: list[str],
        labelCol: str,
        predictionCol: str,
        coefficients: list[float],
        intercept: float,
        logicalVector: LogicalVector | None = None,
    ) -> None:
        super().__init__()
        self._register("featuresCol", "feature columns", featuresCol, TypeConverters.toListString)
        self._register("labelCol", "label column", labelCol)
        self._register("predictionCol", "prediction column", predictionCol)
        self.coefficients = list(coefficients)
        self.intercept = float(intercept)
        self.logicalVector = logicalVector

    def _prediction_expr(self) -> str:
        terms = [f"{c} * {w}" for c, w in zip(self.featuresCol, self.coefficients)]
        return " + ".join([str(self.intercept)] + terms)

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        return df.withColumn(self.predictionCol, expr(self._prediction_expr()))

    def evaluate(self, df: IrisDataFrame) -> Any:
        """PySpark-compatible: score ``df`` and return a ``RegressionSummary``.

        The summary exposes ``.predictions`` plus ``meanAbsoluteError`` /
        ``meanSquaredError`` / ``rootMeanSquaredError`` / ``r2`` / ``numInstances``.
        """
        from irispark.ml.evaluation import RegressionSummary

        return RegressionSummary(
            self, df,
            predictionCol=self.predictionCol,
            labelCol=self.labelCol,
        )


class LinearRegression(Estimator):
    """Linear regression fit with numpy (closed-form normal equations).

    ``fit()`` pulls the feature matrix to numpy and solves for coefficients
    (ridge-regularized for stability); ``transform()`` pushes the learned
    coefficients back as a SQL expression, so scoring never moves data to
    Python (ml_scope §38/§39).
    """

    def __init__(
        self,
        featuresCol,
        labelCol: str,
        predictionCol: str = "prediction",
        regParam: float = 0.0,
        maxIter: int = 100,
    ) -> None:
        super().__init__()
        self._origin_vector = (
            featuresCol if isinstance(featuresCol, LogicalVector) else None
        )
        self._register("featuresCol", "feature columns", resolve_features(featuresCol), TypeConverters.toListString)
        self._register("labelCol", "label column", labelCol)
        self._register("predictionCol", "prediction column", predictionCol)
        self._register("regParam", "L2 regularization", regParam, TypeConverters.toFloat)
        self._register("maxIter", "max iterations", maxIter, TypeConverters.toInt)

    def getFeaturesCol(self) -> list[str]:
        return self.getOrDefault("featuresCol")

    @property
    def logicalVector(self):
        return self._origin_vector

    def setFeaturesCol(self, value: list[str]) -> LinearRegression:
        return self._set("featuresCol", value)

    def getLabelCol(self) -> str:
        return self.getOrDefault("labelCol")

    def setLabelCol(self, value: str) -> LinearRegression:
        return self._set("labelCol", value)

    def getPredictionCol(self) -> str:
        return self.getOrDefault("predictionCol")

    def setPredictionCol(self, value: str) -> LinearRegression:
        return self._set("predictionCol", value)

    def _fit(self, df: IrisDataFrame) -> LinearRegressionModel:
        cols = self.featuresCol + [self.labelCol]
        pdf = df.select(*cols).to_pandas()
        X = pdf[self.featuresCol].to_numpy(dtype=float)
        y = pdf[self.labelCol].to_numpy(dtype=float)
        # Add intercept column
        Xb = np.column_stack([np.ones(len(X)), X])
        lam = self.regParam
        # Ridge: (X^T X + lam*I) beta = X^T y ; do not regularize the intercept
        A = Xb.T @ Xb
        A[0, 0] += 0.0  # intercept unregularized
        if lam > 0:
            A[1:, 1:] += lam * np.eye(X.shape[1])
        b = Xb.T @ y
        try:
            beta = np.linalg.solve(A, b)
        except np.linalg.LinAlgError:
            beta = np.linalg.lstsq(Xb, y, rcond=None)[0]
        intercept = float(beta[0])
        coefficients = [float(v) for v in beta[1:]]
        return LinearRegressionModel(
            featuresCol=self.featuresCol,
            labelCol=self.labelCol,
            predictionCol=self.predictionCol,
            coefficients=coefficients,
            intercept=intercept,
            logicalVector=self._origin_vector,
        )
