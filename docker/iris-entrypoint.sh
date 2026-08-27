#!/bin/bash
# IrisPark IRIS container entrypoint.
# Wraps the official image entrypoint (/tini -- /docker-entrypoint.sh) so the
# instance boots normally (CPF merge included), waits until ready, then applies
# the idempotent seed/init script (namespace, suser user, vendas table).
set -u

/tini -- /docker-entrypoint.sh "$@" &
IRIS_PID=$!

trap 'kill -TERM "$IRIS_PID" 2>/dev/null' SIGTERM SIGINT

echo "[init] waiting for IRIS to accept sessions..."
ready=0
for i in $(seq 1 60); do
    if iris session IRIS -U %SYS "write 1" >/dev/null 2>&1; then
        ready=1
        echo "[init] IRIS ready after ~$((i * 5))s"
        break
    fi
    sleep 5
done

if [ "$ready" -ne 1 ]; then
    echo "[init] ERROR: IRIS did not become ready in time" >&2
    kill -TERM "$IRIS_PID" 2>/dev/null
    exit 1
fi

echo "[init] applying /tmp/init_iris.script..."
if iris session IRIS -U %SYS < /tmp/init_iris.script; then
    echo "[init] done."
else
    echo "[init] WARNING: init script returned non-zero status" >&2
fi

# Server-side Python (used by EPython SQL functions like CB_FIT) must have the
# ML stack available; a freshly created instance ships without it. The
# interpreter lives at /usr/irissys/bin/irispython (not on PATH) and SQL
# functions import from /usr/irissys/mgr/python.
IRISPYTHON=/usr/irissys/bin/irispython
MGR_PY=/usr/irissys/mgr/python
mkdir -p "$MGR_PY/models" "$MGR_PY/AutoML/Classifiers" "$MGR_PY/AutoML/Regressors"
echo "[init] ensuring server-side ML dependencies..."
if ! "$IRISPYTHON" -c "import numpy, sklearn, catboost, iris_automl" >/dev/null 2>&1; then
    if "$IRISPYTHON" -m pip install --no-cache-dir --quiet \
            --target="$MGR_PY" numpy scipy scikit-learn catboost dill patsy psutil statsmodels \
        && "$IRISPYTHON" -m pip install --no-cache-dir --quiet \
            --index-url https://registry.intersystems.com/pypi/simple \
            --target="$MGR_PY" "intersystems-iris-automl==1.0.3"; then
        echo "[init] ML dependencies installed into $MGR_PY."
    else
        echo "[init] ERROR: ML dependency install failed" >&2
    fi
else
    echo "[init] ML dependencies already present."
fi

wait "$IRIS_PID"
