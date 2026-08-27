from __future__ import annotations

import json
import os
import uuid

from irispark.dataframe import IrisDataFrame
from irispark.ml.base import Estimator, Model
from irispark.ml.linalg import LogicalVector, resolve_features
from irispark.ml.param import TypeConverters

# Server-writable directory for pickled sklearn models. Baked into the EPython
# function bodies at CREATE time; override with IRISPARK_MODEL_DIR.
_MODELS_DIR = os.environ.get("IRISPARK_MODEL_DIR", "/usr/irissys/mgr/python/models")
_FIT_FN = "_irispark_sklearn_fit"
_PRED_FN = "_irispark_sklearn_pred"

_FIT_SQL = (
    "CREATE OR REPLACE FUNCTION _irispark_sklearn_fit(p VARCHAR(10000)) "
    "RETURNS VARCHAR(500) LANGUAGE PYTHON {\n"
    "import json, numpy as np, joblib, os\n"
    "from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor\n"
    "from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor\n"
    "reg = {'RandomForestClassifier': RandomForestClassifier, "
    "'RandomForestRegressor': RandomForestRegressor, "
    "'KNeighborsClassifier': KNeighborsClassifier, "
    "'KNeighborsRegressor': KNeighborsRegressor}\n"
    "d = json.loads(p)\n"
    "est = reg[d['kind']](**d.get('params', {}))\n"
    "est.fit(np.array(d['X']), np.array(d['y']))\n"
    "os.makedirs('" + _MODELS_DIR + "', exist_ok=True)\n"
    "path = '" + _MODELS_DIR + "' + '/' + d['name'] + '.joblib'\n"
    "joblib.dump(est, path)\n"
    "return path\n"
    "}"
)

_PRED_SQL = (
    "CREATE OR REPLACE FUNCTION _irispark_sklearn_pred(p VARCHAR(10000)) "
    "RETURNS VARCHAR(2000) LANGUAGE PYTHON {\n"
    "import json, numpy as np, joblib\n"
    "d = json.loads(p)\n"
    "est = joblib.load(d['path'])\n"
    "return json.dumps(est.predict(np.array(d['X'])).tolist())\n"
    "}"
)


def _ensure_functions(session) -> None:
    """Install the EPython fit/predict functions on the session (idempotent)."""
    session.sql(_FIT_SQL)
    session.sql(_PRED_SQL)


def _predict_from_table(df: IrisDataFrame, model_path: str, featuresCol: list[str]) -> list:
    """Run the server-side model on a DataFrame's feature columns; return a JSON list."""
    pdf = df.select(*featuresCol).to_pandas()
    X = pdf[featuresCol].to_numpy(dtype=float).tolist()
    payload = json.dumps({"path": model_path, "X": X})
    rows, _ = df.session.sql(f"SELECT {_PRED_FN}('{payload}')")
    return json.loads(rows[0][0])


def _rows_to_df(df: IrisDataFrame, pdf, columns, session) -> IrisDataFrame:
    """Materialize a pandas result into a temp table-backed DataFrame."""
    tbl = f"irispark_ml_{uuid.uuid4().hex[:8]}"
    col_defs = ", ".join(f'"{c}" VARCHAR(4000)' for c in columns)
    session.sql(f'CREATE TABLE "{tbl}" ({col_defs})')
    str_rows = [tuple("" if v is None else str(v) for v in r) for r in pdf.itertuples(index=False)]
    session._batch_insert(tbl, list(columns), str_rows)
    out = IrisDataFrame(session=session, table_name=tbl)
    out._schema = [(str(c), "VARCHAR(4000)") for c in columns]
    return out


class _EPythonModel(Model):
    """A model fitted server-side via Embedded Python (sklearn backend)."""

    def __init__(
        self,
        featuresCol,
        labelCol,
        predictionCol,
        model_path,
    ) -> None:
        super().__init__()
        self._register("featuresCol", "feature columns", resolve_features(featuresCol), TypeConverters.toListString)
        self._register("labelCol", "label column", labelCol)
        self._register("predictionCol", "prediction column", predictionCol)
        self.model_path = model_path

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        _ensure_functions(df.session)
        preds = _predict_from_table(df, self.model_path, self.featuresCol)
        # Rebuild the output DataFrame with the prediction column aligned to
        # df's row order (predictions match df.select(*features).to_pandas()).
        pdf = df.to_pandas()
        pdf[self.predictionCol] = preds
        return _rows_to_df(df, pdf, list(pdf.columns), df.session)


class _EPythonEstimator(Estimator):
    """Generic estimator fitting a sklearn model inside IRIS via Embedded Python.

    ``fit()`` sends the training features/labels to an EPython function that
    fits the requested sklearn estimator and persists it with joblib on IRIS;
    ``transform()`` loads the model and predicts via EPython. Algorithm identity
    is guaranteed — the requested estimator trains that exact model (ml_scope
    §15/§16), and data movement is minimized by keeping the model server-side.
    """

    _kind = "RandomForestClassifier"
    _default_params: dict = {}

    def __init__(
        self,
        featuresCol,
        labelCol,
        predictionCol: str = "prediction",
        **params,
    ) -> None:
        super().__init__()
        self._origin_vector = (
            featuresCol if isinstance(featuresCol, LogicalVector) else None
        )
        self._register("featuresCol", "feature columns", resolve_features(featuresCol), TypeConverters.toListString)
        self._register("labelCol", "label column", labelCol)
        self._register("predictionCol", "prediction column", predictionCol)
        # Estimator-specific params are stored separately (NOT in self._params,
        # which holds the Param registry). Register each so extractParamMap()
        # and copy() see them.
        self._est_params = dict(self._default_params)
        self._est_params.update(params)
        for k, v in self._est_params.items():
            self._register(k, k, v)

    def getFeaturesCol(self) -> list[str]:
        return self.getOrDefault("featuresCol")

    @property
    def logicalVector(self):
        return self._origin_vector

    def setFeaturesCol(self, value: list[str]) -> _EPythonEstimator:
        return self._set("featuresCol", resolve_features(value))

    def getLabelCol(self) -> str:
        return self.getOrDefault("labelCol")

    def setLabelCol(self, value: str) -> _EPythonEstimator:
        return self._set("labelCol", value)

    def getPredictionCol(self) -> str:
        return self.getOrDefault("predictionCol")

    def setPredictionCol(self, value: str) -> _EPythonEstimator:
        return self._set("predictionCol", value)

    def _fit(self, df: IrisDataFrame) -> _EPythonModel:
        _ensure_functions(df.session)
        cols = self.featuresCol + [self.labelCol]
        pdf = df.select(*cols).to_pandas()
        X = pdf[self.featuresCol].to_numpy(dtype=float).tolist()
        y = pdf[self.labelCol].to_numpy(dtype=float).tolist()
        name = f"{self.__class__.__name__.lower()}_{uuid.uuid4().hex[:8]}"
        payload = json.dumps({"kind": self._kind, "params": self._est_params, "name": name, "X": X, "y": y})
        rows, _ = df.session.sql(f"SELECT {_FIT_FN}('{payload}')")
        path = rows[0][0]
        return _EPythonModel(
            featuresCol=self.featuresCol,
            labelCol=self.labelCol,
            predictionCol=self.predictionCol,
            model_path=path,
        )


class RandomForestClassifier(_EPythonEstimator):
    _kind = "RandomForestClassifier"
    _default_params = {"n_estimators": 100, "n_jobs": 1, "random_state": 0}


class KNeighborsClassifier(_EPythonEstimator):
    _kind = "KNeighborsClassifier"
    _default_params = {"n_neighbors": 5, "n_jobs": 1}


class RandomForestRegressor(_EPythonEstimator):
    _kind = "RandomForestRegressor"
    _default_params = {"n_estimators": 100, "n_jobs": 1, "random_state": 0}


class KNeighborsRegressor(_EPythonEstimator):
    _kind = "KNeighborsRegressor"
    _default_params = {"n_neighbors": 5, "n_jobs": 1}


__all__ = [
    "RandomForestClassifier",
    "KNeighborsClassifier",
    "RandomForestRegressor",
    "KNeighborsRegressor",
]
