"""CatBoost backend: EPython fit/predict functions for notebook 30.

Installs two EPython SQL functions (cb_fit / cb_pred) on the session so
CatBoost can be trained and scored entirely server-side via Embedded Python.
"""

import json

import numpy as np


def ensure_catboost_functions(session) -> None:
    """Create cb_fit/cb_pred EPython SQL functions on the session (idempotent)."""
    fit_sql = (
        "CREATE OR REPLACE FUNCTION cb_fit(p VARCHAR(10000)) "
        "RETURNS VARCHAR(500) LANGUAGE PYTHON {\n"
        "import json, numpy as np, os\n"
        "from catboost import CatBoostClassifier\n"
        "d = json.loads(p)\n"
        "X = np.array(d['X']); y = np.array(d['y'])\n"
        "model = CatBoostClassifier(\n"
        "    iterations=d.get('iterations', 100),\n"
        "    depth=d.get('depth', 6), verbose=0\n"
        ")\n"
        "model.fit(X, y)\n"
        "model_dir = os.environ.get('IRISPARK_MODEL_DIR', '/usr/irissys/mgr/python/models')\n"
        "path = model_dir + '/' + d['name'] + '.cbm'\n"
        "os.makedirs(model_dir, exist_ok=True)\n"
        "model.save_model(path)\n"
        "return path\n"
        "}"
    )
    pred_sql = (
        "CREATE OR REPLACE FUNCTION cb_pred(p VARCHAR(10000)) "
        "RETURNS VARCHAR(5000) LANGUAGE PYTHON {\n"
        "import json, numpy as np\n"
        "from catboost import CatBoostClassifier\n"
        "d = json.loads(p)\n"
        "model = CatBoostClassifier()\n"
        "model.load_model(d['path'])\n"
        "pred = model.predict(np.array(d['X'])).astype(float).tolist()\n"
        "proba = model.predict_proba(np.array(d['X']))[:, 1].tolist()\n"
        "return json.dumps({'pred': pred, 'proba': proba})\n"
        "}"
    )
    session.sql(fit_sql)
    session.sql(pred_sql)


def fit_catboost(session, name, X, y, iterations=100):
    """Fit a CatBoostClassifier server-side via Embedded Python.

    Sends training data as JSON to the cb_fit EPython function.
    Returns the saved model path.
    """
    ensure_catboost_functions(session)
    payload = json.dumps({
        "name": name,
        "X": np.asarray(X).tolist(),
        "y": np.asarray(y).tolist(),
        "iterations": iterations,
    })
    rows, _ = session.sql("SELECT cb_fit('" + payload.replace("'", "''") + "')")
    return rows[0][0]


def predict_catboost(session, model_path, X):
    """Predict using a previously trained CatBoost model."""
    ensure_catboost_functions(session)
    payload = json.dumps({"path": model_path, "X": np.asarray(X).tolist()})
    rows, _ = session.sql("SELECT cb_pred('" + payload.replace("'", "''") + "')")
    return json.loads(rows[0][0])
