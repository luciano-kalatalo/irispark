from __future__ import annotations

import time
import uuid

from irispark.dataframe import IrisDataFrame
from irispark.ml.base import Estimator, Model
from irispark.ml.param import TypeConverters


def _model_name(prefix: str) -> str:
    import uuid

    return f"{prefix}_{uuid.uuid4().hex[:8]}"


class AutoMLModel(Model):
    """A trained IntegratedML AutoML model; predicts via SQL pushdown."""

    def __init__(
        self,
        session,
        modelName: str,
        featuresCol: list[str],
        labelCol: str,
        predictionCol: str,
    ) -> None:
        super().__init__()
        self._register("featuresCol", "feature columns", featuresCol, TypeConverters.toListString)
        self._register("labelCol", "label column", labelCol)
        self._register("predictionCol", "prediction column", predictionCol)
        self._session = session
        self.modelName = modelName

    def _transform(self, df: IrisDataFrame) -> IrisDataFrame:
        # PREDICT(model) is a SQL function; wrap the input as a subquery.
        src = f"({df.to_sql()}) AS _am"
        sql = f"SELECT *, PREDICT({self.modelName}) AS {self.predictionCol} FROM {src}"
        # The training session's socket may be desynced by the TRAIN timeout;
        # run PREDICT on a fresh connection. The returned DataFrame owns this
        # connection (it stays open for subsequent actions).
        if self._session is None:
            raise ValueError("AutoML model has no session; train with a session attached")
        cfg = self._session._config
        probe = self._session.__class__(
            host=cfg["host"],
            port=cfg["port"],
            namespace=cfg["namespace"],
            username=cfg["username"],
            password=cfg["password"],
        )
        rows, columns = probe.sql(sql)
        return _rows_to_df(df, rows, columns, probe)


def _rows_to_df(df: IrisDataFrame, rows, columns, session=None) -> IrisDataFrame:
    """Materialize a query result into a temp table-backed DataFrame.

    ``session`` may be a fresh connection (the training session's socket can be
    desynced by the TRAIN timeout); it is used for the CREATE/INSERT so those
    statements do not hit a broken socket.
    """
    import uuid

    sess = session or df.session
    tbl = f"irispark_automl_{uuid.uuid4().hex[:8]}"
    col_defs = ", ".join(f'"{c}" VARCHAR(4000)' for c in columns)
    sess.sql(f'CREATE TABLE "{tbl}" ({col_defs})')
    # Coerce values to str so pyarrow can serialize into VARCHAR columns
    # (Decimal/float/None values would otherwise break pa.array).
    str_rows = [tuple("" if v is None else str(v) for v in row) for row in rows]
    sess._batch_insert(tbl, list(columns), str_rows)
    # The returned DataFrame must use the fresh connection as its session,
    # because the training session's socket is desynced by the TRAIN timeout.
    out = IrisDataFrame(session=sess, table_name=tbl)
    out._schema = [(str(c), "VARCHAR(4000)") for c in columns]
    return out


class _AutoMLBase(Estimator):
    """Shared IntegratedML AutoML training logic (ml_scope §19/§51).

    ``fit()`` issues CREATE MODEL + TRAIN MODEL. The client socket read
    timeout can fire before TRAIN returns even though training completes
    server-side, so fit() tolerates that and polls %ML.TrainedModel on a
    fresh connection until the model appears.
    """

    def __init__(
        self,
        featuresCol: list[str],
        labelCol: str,
        predictionCol: str = "prediction",
        modelName: str | None = None,
        maxTime: int = 60,
        pollInterval: float = 5.0,
        pollTimeout: float = 600.0,
    ) -> None:
        super().__init__()
        self._register("featuresCol", "feature columns", featuresCol, TypeConverters.toListString)
        self._register("labelCol", "label column", labelCol)
        self._register("predictionCol", "prediction column", predictionCol)
        self._register("maxTime", "AutoML max training time (s)", maxTime, TypeConverters.toInt)
        self._register("pollInterval", "poll interval (s)", pollInterval, TypeConverters.toFloat)
        self._register("pollTimeout", "poll timeout (s)", pollTimeout, TypeConverters.toFloat)
        self.modelName = modelName or _model_name(self.__class__.__name__.lower())

    def getFeaturesCol(self) -> list[str]:
        return self.getOrDefault("featuresCol")

    def setFeaturesCol(self, value: list[str]) -> _AutoMLBase:
        return self._set("featuresCol", value)

    def getLabelCol(self) -> str:
        return self.getOrDefault("labelCol")

    def setLabelCol(self, value: str) -> _AutoMLBase:
        return self._set("labelCol", value)

    def getPredictionCol(self) -> str:
        return self.getOrDefault("predictionCol")

    def setPredictionCol(self, value: str) -> _AutoMLBase:
        return self._set("predictionCol", value)

    def _new_session(self, session):
        """Open a fresh, independent connection (training desyncs the socket)."""
        cfg = session._config
        return session.__class__(
            host=cfg["host"],
            port=cfg["port"],
            namespace=cfg["namespace"],
            username=cfg["username"],
            password=cfg["password"],
        )

    def _fit(self, df: IrisDataFrame) -> AutoMLModel:
        caller = df.session
        # Materialize the training frame (it may be a transformed pipeline, not
        # a physical table) so TRAIN MODEL reads the exact training rows.
        train_tbl = f"irispark_automl_train_{uuid.uuid4().hex[:8]}"
        df.write.mode("overwrite").saveAsTable(train_tbl)

        # Run all training DDL on a dedicated connection: TRAIN MODEL always
        # times out the client socket (~2s) and desyncs that connection, so the
        # caller's session must never touch it.
        work = self._new_session(caller)
        try:
            features = ", ".join(f"{c} numeric" for c in self.featuresCol)
            work.sql(
                f"CREATE MODEL {self.modelName} PREDICTING ({self.labelCol}) WITH ({features})"
            )
            try:
                work.sql(
                    f'TRAIN MODEL {self.modelName} FROM {train_tbl} '
                    f'USING {{"MaxTime": {self.maxTime}}}'
                )
            except Exception:
                # Client socket read timeout; training continues server-side.
                pass
            try:
                work.sql(f"DROP TABLE IF EXISTS {train_tbl}")
            except Exception:
                pass
        finally:
            try:
                work.close()
            except Exception:
                # The training connection's socket is desynced by the TRAIN
                # timeout; its close() may raise EPIPE, which is harmless here.
                pass

        # Poll %ML.TrainedModel on fresh connections until the model appears.
        deadline = time.time() + self.pollTimeout
        while time.time() < deadline:
            try:
                probe = self._new_session(caller)
                rows, _ = probe.sql("SELECT ModelName FROM %ML.TrainedModel")
                probe.close()
                if any(self.modelName in str(r[0]) for r in rows):
                    return AutoMLModel(
                        session=caller,
                        modelName=self.modelName,
                        featuresCol=self.featuresCol,
                        labelCol=self.labelCol,
                        predictionCol=self.predictionCol,
                    )
            except Exception:
                pass
            time.sleep(self.pollInterval)
        raise TimeoutError(
            f"AutoML training for {self.modelName!r} did not complete within "
            f"{self.pollTimeout}s"
        )


class AutoMLClassifier(_AutoMLBase):
    """IntegratedML AutoML for classification (IRIS extension, ml_scope §19)."""


class AutoMLRegressor(_AutoMLBase):
    """IntegratedML AutoML for regression (IRIS extension, ml_scope §19)."""
