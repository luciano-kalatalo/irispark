from __future__ import annotations

import json
import uuid
from typing import Any

from irispark.ml.base import Model, Params

# ─────────────────────────── model registry ───────────────────────────

def _model_registry() -> dict[str, type]:
    """Map model class name -> class for reconstruction on load."""
    from irispark.ml import (
        automl,
        classification,
        feature,
        pipeline,
        regression,
        tuning,
    )

    registry: dict[str, type] = {}
    for mod in (automl, classification, feature, pipeline, regression, tuning):
        for name in dir(mod):
            obj = getattr(mod, name)
            if isinstance(obj, type) and issubclass(obj, Model) and obj is not Model:
                registry[name] = obj
    # PipelineModel is a fitted Transformer (not a Model) but is persistable.
    registry["PipelineModel"] = pipeline.PipelineModel
    return registry


# ─────────────────────────── serialization ───────────────────────────

_SKIP_ATTRS = {
    "_params", "_paramValues", "_paramDocs", "_fitParams",
    "_backend", "_session", "_origin_vector",
}


def _to_jsonable(obj: Any) -> Any:
    """Recursively convert a model/state to JSON-serializable form."""
    if isinstance(obj, Model):
        return {"__model__": _model_state(obj)}
    if isinstance(obj, Params):
        # Non-Model Params (e.g. a Transformer stage in a pipeline): serialize
        # by class + param map so it can be reconstructed.
        return {
            "__params__": {
                "class": obj.__class__.__name__,
                "params": obj.extractParamMap(),
            }
        }
    if obj.__class__.__name__ == "LogicalVector":
        # LogicalVector is a plain metadata class (columns, vectorType, metadata).
        return {
            "__logicalvector__": {
                "columns": list(obj.columns),
                "vectorType": obj.vectorType,
                "metadata": dict(getattr(obj, "metadata", {}) or {}),
            }
        }
    if isinstance(obj, list):
        return [_to_jsonable(x) for x in obj]
    if isinstance(obj, tuple):
        return [_to_jsonable(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    return obj


def _model_state(model: Model) -> dict[str, Any]:
    """Serialize a fitted model: class name + params + learned attributes.

    PipelineModel is a Transformer (not a Model) but is a fitted artifact; its
    ``_stages`` (nested Models) and ``_backends`` are serialized recursively.
    """
    params = model.extractParamMap()
    learned = {
        k: _to_jsonable(v)
        for k, v in model.__dict__.items()
        if k not in _SKIP_ATTRS and not k.startswith("_")
    }
    # PipelineModel: serialize nested stages + backends explicitly
    if hasattr(model, "_stages"):
        learned["_stages"] = _to_jsonable(list(model._stages))
    if hasattr(model, "_backends"):
        learned["_backends"] = dict(model._backends)
    return {"class": model.__class__.__name__, "params": params, "learned": learned}


def _from_jsonable(obj: Any, registry: dict[str, type]) -> Any:
    """Recursively reconstruct a model/state from JSON form."""
    if isinstance(obj, dict) and "__model__" in obj:
        return _from_state(obj["__model__"], registry)
    if isinstance(obj, dict) and "__params__" in obj:
        info = obj["__params__"]
        cls = registry.get(info["class"])
        if cls is None:
            # Non-Model Params (e.g. a Transformer stage): reconstruct from
            # its class name + param map.
            from irispark.ml import feature, pipeline

            for mod in (feature, pipeline):
                c = getattr(mod, info["class"], None)
                if c is not None:
                    cls = c
                    break
        if cls is None:
            raise ValueError(f"Unknown class {info['class']!r}")
        return cls(**info["params"])
    if isinstance(obj, dict) and "__logicalvector__" in obj:
        from irispark.ml.linalg import LogicalVector

        info = obj["__logicalvector__"]
        return LogicalVector(
            columns=info["columns"],
            vectorType=info.get("vectorType", "dense"),
            metadata=info.get("metadata", {}),
        )
    if isinstance(obj, list):
        return [_from_jsonable(x, registry) for x in obj]
    if isinstance(obj, dict):
        return {k: _from_jsonable(v, registry) for k, v in obj.items()}
    return obj


def _from_state(state: dict[str, Any], registry: dict[str, type]) -> Model:
    """Reconstruct a fitted model from its serialized state.

    Model constructors take both params (featuresCol, labelCol, ...) and
    learned state (coefficients, intercept, ...), so merge them into one
    kwargs dict. Learned values are reconstructed recursively (they may
    themselves be nested Models, e.g. PipelineModel stages).
    """
    cls = registry[state["class"]]
    params = dict(state.get("params", {}))
    learned = state.get("learned", {})
    # PipelineModel stores stages/backends under _stages/_backends but its
    # constructor takes stages/backends.
    if cls.__name__ == "PipelineModel":
        if "_stages" in learned:
            learned["stages"] = learned.pop("_stages")
        if "_backends" in learned:
            learned["backends"] = learned.pop("_backends")
    # Split learned state into constructor params vs plain attributes: some
    # learned values are required ctor args (coefficients, intercept), others
    # are metadata set after construction (backend, logicalVector).
    import inspect

    sig = inspect.signature(cls.__init__)  # type: ignore[misc]  # dynamic reconstruction from JSON
    ctor_args = set(sig.parameters) - {"self"}
    ctor_kwargs: dict[str, Any] = {}
    attr_values: dict[str, Any] = {}
    for k, v in learned.items():
        (ctor_kwargs if k in ctor_args else attr_values)[k] = _from_jsonable(v, registry)
    model = cls(**params, **ctor_kwargs)
    for k, v in attr_values.items():
        setattr(model, k, v)
    return model


# ─────────────────────────── persistence ───────────────────────────

_SCHEMA = "IRISML"
_TABLE = "Model"


def _ensure_table(session) -> None:
    """Create the IRISpark_ML.Model table if it does not exist."""
    session.sql(
        f"CREATE TABLE IF NOT EXISTS {_SCHEMA}.{_TABLE} ("
        f"id VARCHAR(64) NOT NULL, "
        f"name VARCHAR(255), "
        f"class VARCHAR(255), "
        f"state VARCHAR(1000000), "
        f"created TIMESTAMP, "
        f"PRIMARY KEY (id))"
    )


def save(model: Model, name: str, session) -> str:
    """Persist a fitted model to IRISpark_ML.Model; returns the model id."""
    _ensure_table(session)
    state = _model_state(model)
    mid = uuid.uuid4().hex[:16]
    payload = json.dumps(state)
    session.sql(
        f"INSERT INTO {_SCHEMA}.{_TABLE} (id, name, class, state, created) "
        f"VALUES ('{mid}', '{name}', '{state['class']}', "
        f"'{payload.replace(chr(39), chr(39) + chr(39))}', CURRENT_TIMESTAMP)"
    )
    return mid


def load(model_id: str, session) -> Model:
    """Reconstruct a fitted model from its stored state."""
    _ensure_table(session)
    rows, _ = session.sql(
        f"SELECT state FROM {_SCHEMA}.{_TABLE} WHERE id = '{model_id}'"
    )
    if not rows:
        raise ValueError(f"No model with id {model_id!r}")
    state = json.loads(rows[0][0])
    return _from_state(state, _model_registry())


def load_by_name(name: str, session) -> Model:
    """Reconstruct the most recently saved model with the given name."""
    _ensure_table(session)
    rows, _ = session.sql(
        f"SELECT id, state FROM {_SCHEMA}.{_TABLE} "
        f"WHERE name = '{name}' ORDER BY created DESC"
    )
    if not rows:
        raise ValueError(f"No model named {name!r}")
    return _from_state(json.loads(rows[0][1]), _model_registry())


def list_models(session) -> list[dict[str, Any]]:
    """List saved models (id, name, class, created)."""
    _ensure_table(session)
    rows, _ = session.sql(
        f"SELECT id, name, class, created FROM {_SCHEMA}.{_TABLE} ORDER BY created DESC"
    )
    return [
        {"id": r[0], "name": r[1], "class": r[2], "created": r[3]} for r in rows
    ]


def delete_model(model_id: str, session) -> None:
    """Delete a saved model by id."""
    _ensure_table(session)
    session.sql(f"DELETE FROM {_SCHEMA}.{_TABLE} WHERE id = '{model_id}'")
