from __future__ import annotations

import os
import time
import uuid

from irispark.dataframe import IrisDataFrame
from irispark.ml.base import Estimator, Model
from irispark.ml.linalg import resolve_features
from irispark.ml.param import TypeConverters

# Server-writable AutoML discovery directories. Baked into the install SQL at
# runtime; override the base with IRISPARK_AUTOML_DIR.
_AUTOML_BASE = os.environ.get("IRISPARK_AUTOML_DIR", "/usr/irissys/mgr/python/AutoML")
_CLASSIFIERS_DIR = _AUTOML_BASE + "/Classifiers"
_REGRESSORS_DIR = _AUTOML_BASE + "/Regressors"


def _knn_wrapper_source(kind: str, params: dict) -> str:
    """Return an IRISModel Python file that AutoML can discover and select.

    Follows the documented GIML_Custom contract: an ``IRISModel`` class wrapping
    a sklearn ``BaseEstimator``, referenced by ``name`` in CREATE MODEL/TRAIN
    MODEL. See docs/LESSONS_LEARNED for the AutoML-availability remedies.
    """
    n_neighbors = params.get("n_neighbors", 5)
    return (
        "import numpy as np\n"
        "from sklearn.neighbors import KNeighborsClassifier\n"
        "from sklearn.base import BaseEstimator, ClassifierMixin\n\n"
        "class IRISModel:\n"
        "    def __init__(self, **kwargs):\n"
        "        n_neighbors = kwargs.get('n_neighbors', " + str(n_neighbors) + ")\n"
        "        self.model = KNNWrapper(n_neighbors=n_neighbors, n_jobs=1)\n"
        "        self.name = '" + kind + "'\n"
        "        self.model_type = 'KNN Model'\n\n"
        "class KNNWrapper(ClassifierMixin, BaseEstimator):\n"
        "    def __init__(self, **kwargs):\n"
        "        self.knn = KNeighborsClassifier(**kwargs)\n"
        "    def fit(self, X, y):\n"
        "        self.classes_ = np.unique(y)\n"
        "        self.knn.fit(X, y)\n"
        "        return self\n"
        "    def predict(self, X):\n"
        "        return self.knn.predict(X)\n"
        "    def predict_proba(self, X):\n"
        "        return self.knn.predict_proba(X)\n"
        "    def get_params(self, deep=True):\n"
        "        return self.knn.get_params(deep)\n"
        "    def set_params(self, **params):\n"
        "        self.knn.set_params(**params)\n"
        "        return self\n"
    )


def _install_custom_model(session, name: str, problem: str, params: dict) -> str:
    """Write the IRISModel wrapper file into the AutoML directory (server-side).

    The wrapper source is base64-encoded (newlines/quotes can't be embedded in
    a SQL string) and decoded inside Embedded Python before writing, because
    the AutoML directories live on the IRIS server filesystem.
    """
    import base64

    directory = _CLASSIFIERS_DIR if problem == "classification" else _REGRESSORS_DIR
    source = _knn_wrapper_source(name, params)
    encoded = base64.b64encode(source.encode()).decode()
    session.sql(
        "CREATE OR REPLACE FUNCTION _irispark_install_custom(p VARCHAR(10000)) "
        "RETURNS INT LANGUAGE PYTHON {\n"
        "import os, base64\n"
        "directory = '" + directory + "'\n"
        "os.makedirs(directory, exist_ok=True)\n"
        "data = base64.b64decode(p).decode()\n"
        "with open(directory + '/" + name + ".py', 'w') as f:\n"
        "    f.write(data)\n"
        "return 1\n"
        "}"
    )
    rows, _ = session.sql(f"SELECT _irispark_install_custom('{encoded}')")
    return directory + "/" + name + ".py"


class CustomModelModel(Model):
    """A model trained via the IntegratedML AutoML provider using a custom IRISModel."""

    def __init__(self, session, model_name, featuresCol, labelCol, predictionCol) -> None:
        super().__init__()
        self._session = session
        self.model_name = model_name
        self._register("featuresCol", "feature columns", resolve_features(featuresCol), TypeConverters.toListString)
        self._register("labelCol", "label column", labelCol)
        self._register("predictionCol", "prediction column", predictionCol)

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        if self._session is None:
            raise ValueError("Custom model has no session; train with a session attached")
        cfg = self._session._config
        probe = self._session.__class__(
            host=cfg["host"], port=cfg["port"], namespace=cfg["namespace"],
            username=cfg["username"], password=cfg["password"],
        )
        src = f"({df.to_sql()}) AS _cm"
        sql = f"SELECT *, PREDICT({self.model_name}) AS {self.predictionCol} FROM {src}"
        rows, columns = probe.sql(sql)
        # The returned DataFrame owns the probe connection (it stays open for
        # subsequent actions).
        return _rows_to_df(df, rows, columns, probe)


def _rows_to_df(df, rows, columns, session) -> IrisDataFrame:
    import uuid as _u

    tbl = f"irispark_custom_{_u.uuid4().hex[:8]}"
    col_defs = ", ".join(f'"{c}" VARCHAR(4000)' for c in columns)
    session.sql(f'CREATE TABLE "{tbl}" ({col_defs})')
    str_rows = [tuple("" if v is None else str(v) for v in r) for r in rows]
    session._batch_insert(tbl, list(columns), str_rows)
    out = IrisDataFrame(session=session, table_name=tbl)
    out._schema = [(str(c), "VARCHAR(4000)") for c in columns]
    return out


class CustomModelClassifier(Estimator):
    """Train a custom IRISModel via the IntegratedML AutoML provider.

    Writes the wrapper file into AutoML/Classifiers and references it by name
    in CREATE MODEL / TRAIN MODEL, executed via Embedded Python. AutoML selects
    among candidate models (ml_scope §19). Apply the LESSONS remedies: use
    enough training rows and poll a fresh connection for completion.
    """

    def __init__(
        self,
        featuresCol,
        labelCol,
        predictionCol: str = "prediction",
        n_neighbors: int = 5,
        maxTime: int = 2,
        pollTimeout: float = 90.0,
    ) -> None:
        super().__init__()
        self._register("featuresCol", "feature columns", resolve_features(featuresCol), TypeConverters.toListString)
        self._register("labelCol", "label column", labelCol)
        self._register("predictionCol", "prediction column", predictionCol)
        self._register("n_neighbors", "KNN neighbors", n_neighbors, TypeConverters.toInt)
        self._register("maxTime", "AutoML MaxTime (minutes, TrainMode=TIME)", maxTime, TypeConverters.toInt)
        self._register("pollTimeout", "poll timeout (s)", pollTimeout, TypeConverters.toFloat)
        self.model_name = f"irispark_custom_{uuid.uuid4().hex[:8]}"

    def _new_session(self, session):
        cfg = session._config
        return session.__class__(
            host=cfg["host"], port=cfg["port"], namespace=cfg["namespace"],
            username=cfg["username"], password=cfg["password"],
        )

    def _fit(self, df: IrisDataFrame) -> CustomModelModel:
        caller = df.session
        # Materialize training frame (transformed pipeline or plain table).
        train_tbl = f"irispark_custom_train_{uuid.uuid4().hex[:8]}"
        df.write.mode("overwrite").saveAsTable(train_tbl)
        # Write the wrapper file.
        _install_custom_model(caller, self.model_name, "classification",
                              {"n_neighbors": self.n_neighbors})
        # Train on a dedicated connection (TRAIN MODEL desyncs the socket).
        work = self._new_session(caller)
        try:
            features = ", ".join(f"{c} numeric" for c in self.featuresCol)
            work.sql(
                f"CREATE MODEL {self.model_name} PREDICTING ({self.labelCol}) "
                f"WITH ({features})"
            )
            try:
                # MaxTime is MINUTES (GIML_Configuration_Providers) and is only
                # honored when TrainMode is "TIME"; the default TrainMode
                # ("SCORE") ignores it entirely, which previously let a cold
                # instance block training indefinitely.
                work.sql(
                    f"TRAIN MODEL {self.model_name} FROM {train_tbl} "
                    f"USING {{\"TrainMode\": \"TIME\", \"MaxTime\": {self.maxTime}}}"
                )
            except Exception:
                pass  # client socket read timeout; training continues server-side
            try:
                work.sql(f"DROP TABLE IF EXISTS {train_tbl}")
            except Exception:
                pass
        finally:
            try:
                work.close()
            except Exception:
                pass
        # Poll %ML.TrainedModel on fresh connections.
        deadline = time.time() + self.pollTimeout
        while time.time() < deadline:
            try:
                probe = self._new_session(caller)
                rows, _ = probe.sql("SELECT ModelName FROM %ML.TrainedModel")
                probe.close()
                if any(self.model_name in str(r[0]) for r in rows):
                    return CustomModelModel(
                        session=caller,
                        model_name=self.model_name,
                        featuresCol=self.featuresCol,
                        labelCol=self.labelCol,
                        predictionCol=self.predictionCol,
                    )
            except Exception:
                pass
            time.sleep(5)
        raise TimeoutError(
            f"AutoML custom-model training for {self.model_name!r} did not "
            f"complete within {self.pollTimeout}s"
        )


__all__ = ["CustomModelClassifier", "CustomModelModel"]
