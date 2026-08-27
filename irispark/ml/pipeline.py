from __future__ import annotations

from typing import Any

from irispark.dataframe import IrisDataFrame
from irispark.ml.base import Estimator, Transformer
from irispark.ml.param import Param, Params


class Pipeline(Estimator):
    """
    A simple pipeline, which acts as an estimator. A Pipeline consists
    of a sequence of stages, each of which is either an Estimator or a Transformer.
    When Pipeline.fit() is called, the stages are executed in order.
    """

    stages: Param[list[Transformer | Estimator]] = Param(
        parent=Params(),
        name="stages",
        doc="A list of stages. Each stage is either an Estimator or a Transformer.",
        defaultValue=[],
    )

    def __init__(self, stages: list[Transformer | Estimator] | None = None) -> None:
        super().__init__()
        self._stages: list[Transformer | Estimator] = stages or []

    def setStages(self, value: list[Transformer | Estimator]) -> Pipeline:
        """Sets the stages of the pipeline."""
        self._stages = value
        return self

    def getStages(self) -> list[Transformer | Estimator]:
        """Returns the stages of the pipeline."""
        return self._stages

    def _fit(self, df: IrisDataFrame) -> PipelineModel:  # type: ignore[override]  # PipelineModel is a Transformer, narrower than Model (ml_scope §41)
        """Fits the pipeline to the input DataFrame.

        Each stage transforms the DataFrame in place, so later stages see the
        output of earlier ones (e.g. a VectorAssembler's derived column).
        """
        stages = self._stages
        model_stages: list[Transformer] = []
        backends: dict[str, str] = {}
        for stage in stages:
            if isinstance(stage, Estimator):
                model = stage.fit(df)
                model_stages.append(model)
                df = model.transform(df)
            else:
                df = stage.transform(df)
                model_stages.append(stage)
            backend = getattr(stage, "_resolve_backend", lambda: None)()
            if backend:
                backends[stage.__class__.__name__] = backend
        return PipelineModel(stages=model_stages, backends=backends)


class PipelineModel(Transformer):
    """
    Represents a fitted pipeline.
    """

    def __init__(
        self,
        stages: list[Transformer] | None = None,
        backends: dict[str, str] | None = None,
    ) -> None:
        super().__init__()
        self._stages: list[Transformer] = stages or []
        self._backends: dict[str, str] = backends or {}

    def getStages(self) -> list[Transformer]:
        """Returns the stages of the pipeline."""
        return self._stages

    def getBackends(self) -> dict[str, str]:
        """Stage class name -> planner-assigned backend (ml_scope §41)."""
        return dict(self._backends)

    def getLogicalVectors(self) -> list:
        """LogicalVectors contributed by the stages, in order (ml_scope §9)."""
        vectors = []
        for stage in self._stages:
            lv = getattr(stage, "logicalVector", None) or getattr(
                stage, "_origin_vector", None
            )
            if lv is not None:
                vectors.append(lv)
        return vectors

    def save(self, name: str, session=None) -> str:
        """Persist this fitted pipeline to IRISpark_ML.Model; returns the model id."""
        from irispark.ml.persistence import save as _save

        sess = session or getattr(self, "_session", None)
        if sess is None:
            raise ValueError("save() requires a session (pass session=...)")
        return _save(self, name, sess)  # type: ignore[arg-type]  # PipelineModel is a fitted Transformer, not a Model

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        """Applies the pipeline stages to the input DataFrame."""
        for stage in self._stages:
            df = stage.transform(df)
        return df

    def copy(self, extra: dict[str, Any] | None = None) -> PipelineModel:
        new = self.__class__(stages=self._stages.copy())
        new._paramValues = self._paramValues.copy()
        if extra:
            for k, v in extra.items():
                self._setParam(k, v)
        return self
