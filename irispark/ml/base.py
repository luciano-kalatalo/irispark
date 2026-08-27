from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, TypeVar

from irispark.dataframe import IrisDataFrame
from irispark.ml.param import Params

if TYPE_CHECKING:
    from irispark.session import IrisParkSession


class Transformer(Params):
    """
    Base class for transformers that transform one DataFrame into another.

    Transformer is an abstraction for feature transformers and models.
    """

    def __init__(self) -> None:
        super().__init__()
        self._backend: str | None = None

    @property
    def backend(self) -> str | None:
        """Planner-assigned backend for this stage, if available."""
        return self._backend

    @backend.setter
    def backend(self, value: str | None) -> None:
        self._backend = value

    @abstractmethod
    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        """Transforms the input DataFrame."""
        ...

    def _resolve_backend(self) -> str | None:
        """Return the planner-assigned backend for this stage, if available."""
        try:
            from irispark.ml.planner import default_planner

            return default_planner.resolve_backend(self)
        except Exception:
            return None

    def transform(self, df: IrisDataFrame) -> IrisDataFrame:
        """Transforms the input DataFrame."""
        return self._transform(df)

    def transformSchema(self, schema: dict[str, str]) -> dict[str, str]:
        """Validates and transforms the schema."""
        return schema


T = TypeVar("T", bound="Model")


class Estimator(Params, ABC):
    """
    Base class for estimators that fit a model to a DataFrame.
    """

    def __init__(self) -> None:
        super().__init__()

    def _resolve_backend(self) -> str | None:
        """Return the planner-assigned backend for this estimator, if available."""
        try:
            from irispark.ml.planner import default_planner

            return default_planner.resolve_backend(self)
        except Exception:
            return None

    @abstractmethod
    def _fit(self, df: IrisDataFrame) -> Model:
        """Fits a model to the input DataFrame."""
        ...

    def fit(self, df: IrisDataFrame) -> Model:
        """Fits a model to the input DataFrame."""
        model = self._fit(df)
        model._fitParams = self.extractParamMap()
        backend = self._resolve_backend()
        if backend:
            model.backend = backend
        # Remember the training session so Model.save() and friends can reach
        # IRIS without an explicit session argument.
        try:
            model._session = df.session
        except AttributeError:
            pass
        return model

    def fitMultiple(self, datasets: list[IrisDataFrame]) -> list[Model]:
        """Fits multiple models to multiple DataFrames."""
        return [self.fit(df) for df in datasets]

    def copy(self, extra: dict[str, Any] | None = None) -> Estimator:
        """Creates a copy of this estimator with extra parameters.

        The copy is reconstructed from the current param values (passed as
        constructor kwargs) so it is independent of the original — important
        for grid search, which must not mutate the user's estimator.
        """
        values = dict(self._paramValues)
        if extra:
            for k, v in extra.items():
                values[k] = v
        new = self.__class__(**values)
        return new


M = TypeVar("M", bound="Model")


class Model(Transformer, ABC):
    """
    Base class for models fitted by an Estimator.
    """

    def __init__(self) -> None:
        super().__init__()
        self._fitParams: dict[str, Any] = {}
        self._session: IrisParkSession | None = None

    @property
    def fitParams(self) -> dict[str, Any]:
        return self._fitParams

    def evaluate(self, df: IrisDataFrame) -> Any:
        """PySpark-compatible evaluation.

        Concrete models override this to wrap ``transform(df)`` in a summary
        object exposing ``.predictions`` and metric accessors
        (e.g. ``meanAbsoluteError`` / ``areaUnderROC``).
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement evaluate()"
        )

    def copy(self, extra: dict[str, Any] | None = None) -> Model:
        """Creates a copy of this model with extra parameters."""
        new = self.__class__()
        for p in self._params.values():
            new._params[p.name] = p
        new._paramValues = self._paramValues.copy()
        new._fitParams = self._fitParams.copy()
        if extra:
            for k, v in extra.items():
                new._setParam(k, v)
        return new

    @abstractmethod
    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        """Transforms the input DataFrame."""
        ...

    def save(self, name: str, session=None) -> str:
        """Persist this fitted model to IRISpark_ML.Model; returns the model id.

        ``session`` defaults to the model's training session when available.
        """
        from irispark.ml.persistence import save as _save

        sess = session or getattr(self, "_session", None)
        if sess is None:
            raise ValueError("save() requires a session (pass session=...)")
        return _save(self, name, sess)
