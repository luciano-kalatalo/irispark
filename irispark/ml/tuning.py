from __future__ import annotations

from collections.abc import Callable
from itertools import product
from typing import Any

from irispark.dataframe import IrisDataFrame
from irispark.ml.base import Estimator, Model
from irispark.ml.param import Param, Params, TypeConverters


class ParamGridBuilder(Params):
    """Build a grid of parameter values for tuning (ml_scope §30).

    ``addGrid(param, values)`` registers a param + candidate values; ``build()``
    returns the Cartesian product as a list of param-name -> value dicts.
    """

    def __init__(self) -> None:
        super().__init__()
        self._grid: dict[str, list[Any]] = {}
        self._converters: dict[str, Callable[[Any], Any]] = {}

    def addGrid(self, param, values: list[Any]) -> ParamGridBuilder:
        name = param.name if isinstance(param, Param) else str(param)
        converter = (
            param.typeConverter if isinstance(param, Param) else TypeConverters.identity
        )
        self._grid[name] = list(values)
        self._converters[name] = converter
        return self

    def build(self) -> list[dict[str, Any]]:
        if not self._grid:
            return [{}]
        names = list(self._grid.keys())
        combos = product(*(self._grid[n] for n in names))
        maps = []
        for combo in combos:
            m = {}
            for n, v in zip(names, combo):
                m[n] = self._converters[n](v)
            maps.append(m)
        return maps


# Metrics that should be maximized (higher is better). Everything else is
# minimized (regression errors: mse/rmse/mae).
_MAXIMIZE_METRICS = {
    "accuracy", "precision", "recall", "f1",
    "areaunderroc", "areaunderpr", "r2", "r2",
}


def _better(score: float, best: float | None, metric: str) -> bool:
    if best is None:
        return True
    if metric.lower() in _MAXIMIZE_METRICS:
        return score > best
    return score < best


class _BestModel(Model):
    """Thin model that delegates transform to the best fitted estimator's model."""

    def __init__(self, model: Model) -> None:
        super().__init__()
        self._model = model

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        return self._model.transform(df)


class _ValidatorMixin(Params):
    """Shared parameter-tuning selection logic (ml_scope §30/§31)."""

    def __init__(
        self,
        estimator: Estimator,
        estimatorParamMaps: list[dict[str, Any]],
        evaluator,
        seed: int = 42,
    ) -> None:
        super().__init__()
        self._register("estimator", "estimator to tune", estimator)
        self._register(
            "estimatorParamMaps", "grid of parameter maps", estimatorParamMaps
        )
        self._register("evaluator", "evaluator", evaluator)
        self._register("seed", "random seed", seed, TypeConverters.toInt)
        self.estimator = estimator
        self.estimatorParamMaps = estimatorParamMaps
        self.evaluator = evaluator
        self.bestParams: dict[str, Any] = {}
        self.avgMetrics: list[float] = []

    def getMetricName(self) -> str:
        return getattr(self.evaluator, "getMetricName", lambda: "rmse")()


def _combine(dfs) -> IrisDataFrame:
    """Union a list of DataFrames into one (fold reassembly)."""
    if not dfs:
        raise ValueError("no frames to combine")
    result = dfs[0]
    for d in dfs[1:]:
        result = result.union(d)
    return result


def _materialize(df: IrisDataFrame, ref: IrisDataFrame) -> IrisDataFrame:
    """Persist a DataFrame to a temp table (gives it a real %ID)."""
    import uuid

    tbl = f"irispark_cv_fold_{uuid.uuid4().hex[:8]}"
    df.write.mode("overwrite").saveAsTable(tbl)
    out = IrisDataFrame(session=ref.session, table_name=tbl)
    out._schema = df._schema or []
    return out


class CrossValidator(_ValidatorMixin, Estimator):
    """k-fold cross-validator over a grid (ml_scope §31).

    For each param map, fit on k-1 folds and score on the held-out fold, then
    average. The best map is selected by the evaluator's metric direction and
    the estimator is retrained on the full dataset.

    Note: intended for the numpy estimators (LinearRegression /
    LogisticRegression); AutoML training is too expensive to cross-validate.
    """

    def __init__(
        self,
        estimator: Estimator,
        estimatorParamMaps: list[dict[str, Any]],
        evaluator,
        numFolds: int = 3,
        seed: int = 42,
    ) -> None:
        super().__init__(
            estimator=estimator,
            estimatorParamMaps=estimatorParamMaps,
            evaluator=evaluator,
            seed=seed,
        )
        self._register("numFolds", "number of folds", numFolds, TypeConverters.toInt)
        self.numFolds = numFolds

    def _fit(self, df: IrisDataFrame) -> _BestModel:
        n = self.numFolds
        weights = [1.0] * n
        metric = self.getMetricName()
        self.avgMetrics = []
        best_score: float | None = None
        best_model: Model | None = None

        # randomSplit leaves %ID-based MOD filters that break under the
        # prediction/withColumn subquery wrappers (Field '%ID' not found).
        # Materialize each fold to a physical table so it has a real %ID.
        folds = [_materialize(f, df) for f in df.randomSplit(weights, seed=self.seed)]

        for pmap in self.estimatorParamMaps:
            fold_scores = []
            for f, val_df in enumerate(folds):
                train = _combine([x for i, x in enumerate(folds) if i != f])
                est = self.estimator.copy(extra=pmap)
                model = est.fit(train)
                fold_scores.append(self.evaluator.evaluate(model.transform(val_df)))
            avg = sum(fold_scores) / len(fold_scores)
            self.avgMetrics.append(avg)
            if _better(avg, best_score, metric):
                best_score = avg
                best_model = est.fit(df)

        assert best_score is not None
        self.bestParams = self.estimatorParamMaps[self.avgMetrics.index(best_score)]
        assert best_model is not None
        return _BestModel(best_model)


class TrainValidationSplit(_ValidatorMixin, Estimator):
    """Single train/validation split over a grid (ml_scope §30)."""

    def __init__(
        self,
        estimator: Estimator,
        estimatorParamMaps: list[dict[str, Any]],
        evaluator,
        trainRatio: float = 0.75,
        seed: int = 42,
    ) -> None:
        super().__init__(
            estimator=estimator,
            estimatorParamMaps=estimatorParamMaps,
            evaluator=evaluator,
            seed=seed,
        )
        self._register("trainRatio", "train ratio", trainRatio, TypeConverters.toFloat)
        self.trainRatio = trainRatio

    def _fit(self, df: IrisDataFrame) -> _BestModel:
        metric = self.getMetricName()
        train, val = df.randomSplit([self.trainRatio, 1.0 - self.trainRatio], seed=self.seed)
        # Materialize folds (randomSplit's %ID MOD filter breaks under the
        # prediction subquery wrappers).
        train = _materialize(train, df)
        val = _materialize(val, df)
        self.avgMetrics = []
        best_score: float | None = None
        best_model: Model | None = None
        for pmap in self.estimatorParamMaps:
            est = self.estimator.copy(extra=pmap)
            model = est.fit(train)
            score = self.evaluator.evaluate(model.transform(val))
            self.avgMetrics.append(score)
            if _better(score, best_score, metric):
                best_score = score
                best_model = est.fit(df)
        assert best_score is not None
        self.bestParams = self.estimatorParamMaps[self.avgMetrics.index(best_score)]
        assert best_model is not None
        return _BestModel(best_model)
