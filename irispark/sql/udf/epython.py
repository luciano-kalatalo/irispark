"""EPython thunks for irispark_udc — Embedded Python UDF wrappers.

Each thunk is a ``LANGUAGE PYTHON`` SQL UDF whose body imports
``irispark_udc.py`` via ``importlib.util`` so the pure-Python semantics
run inside IRIS's Embedded Python interpreter.

The absolute path to ``irispark_udc.py`` is resolved at DDL generation
time (client-side) and embedded in the thunk body; the IRIS-side Python
must be able to read that path (e.g. via a Docker bind-mount or
installed package).  Environment variable ``IRISPARK_UDC_PATH`` overrides
the auto-resolved path.

Boundary convention (matches ``datetime_ext.py`` / ``irispark_udc.py``):
SQL NULL arrives as ``""`` (empty string) and is returned as ``None``,
which ObjectScript converts back to SQL NULL.
"""

import os
import warnings

_UDC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "irispark_udc.py"))


def _esc(path: str) -> str:
    """Escape backslashes and single quotes for a Python string literal inside DDL."""
    return path.replace("\\", "\\\\").replace("'", "\\'")


def _probe_udc_path(session) -> str | None:
    """Probe IRIS for the best candidate path to irispark_udc.py.

    Priority:
    1. ``IRISPARK_UDC_PATH`` environment variable (explicit override).
    2. ``/repo/irispark/sql/udf/irispark_udc.py`` (dev/CI Docker mount).
    3. Client-side ``_UDC_PATH`` (same-machine IRIS installs).

    Returns ``None`` if none of the candidates exist on the IRIS side.
    """
    env = os.environ.get("IRISPARK_UDC_PATH")
    if env:
        return env

    candidates = ["/repo/irispark/sql/udf/irispark_udc.py", _UDC_PATH]
    probe_fn = "irispark_udc_probe"
    probe_ddl = (
        f"CREATE OR REPLACE FUNCTION {probe_fn}(p VARCHAR(4000)) RETURNS INT "
        f"LANGUAGE PYTHON {{ import os; return 1 if os.path.exists(p) else 0 }}"
    )
    try:
        session.sql(probe_ddl)
        for cand in candidates:
            sql_escaped = cand.replace("'", "''")
            r = session.sql(f"SELECT {probe_fn}('{sql_escaped}')")
            if r.rows and r.rows[0][0] == 1:
                return cand
    except Exception:
        pass
    finally:
        try:
            session.sql(f"DROP FUNCTION {probe_fn}")
        except Exception:
            pass
    return None


def _build_ddl(udc_path: str) -> list[str]:
    """Generate the DDL statements for the current UDC path."""
    ep = _esc(udc_path)
    return [
        (
            "CREATE OR REPLACE FUNCTION irispark_udc_conv(num VARCHAR(4000), frombase INT, tobase INT)\n"
            "RETURNS VARCHAR(4000)\n"
            "LANGUAGE PYTHON\n"
            "{\n"
            "    if num == \"\" or frombase == \"\" or tobase == \"\":\n"
            "        return None\n"
            "    import importlib.util as _u\n"
            f"    _spec = _u.spec_from_file_location('irispark_udc', '{ep}')\n"
            "    _mod = _u.module_from_spec(_spec)\n"
            "    _spec.loader.exec_module(_mod)\n"
            "    return _mod.conv(num, frombase, tobase)\n"
            "}\n"
        ),
        (
            "CREATE OR REPLACE FUNCTION irispark_udc_format_string(\n"
            "    fmt VARCHAR(4000),\n"
            "    a1 VARCHAR(4000), a2 VARCHAR(4000), a3 VARCHAR(4000),\n"
            "    a4 VARCHAR(4000), a5 VARCHAR(4000), a6 VARCHAR(4000),\n"
            "    a7 VARCHAR(4000)\n"
            ")\n"
            "RETURNS VARCHAR(4000)\n"
            "LANGUAGE PYTHON\n"
            "{\n"
            "    if fmt == \"\":\n"
            "        return None\n"
            "    import importlib.util as _u\n"
            f"    _spec = _u.spec_from_file_location('irispark_udc', '{ep}')\n"
            "    _mod = _u.module_from_spec(_spec)\n"
            "    _spec.loader.exec_module(_mod)\n"
            "    args = [a for a in [a1, a2, a3, a4, a5, a6, a7] if a != \"\"]\n"
            "    return _mod.format_string(fmt, *args)\n"
            "}\n"
        ),
        (
            "CREATE OR REPLACE FUNCTION irispark_udc_printf(\n"
            "    fmt VARCHAR(4000),\n"
            "    a1 VARCHAR(4000), a2 VARCHAR(4000), a3 VARCHAR(4000),\n"
            "    a4 VARCHAR(4000), a5 VARCHAR(4000), a6 VARCHAR(4000),\n"
            "    a7 VARCHAR(4000)\n"
            ")\n"
            "RETURNS VARCHAR(4000)\n"
            "LANGUAGE PYTHON\n"
            "{\n"
            "    if fmt == \"\":\n"
            "        return None\n"
            "    import importlib.util as _u\n"
            f"    _spec = _u.spec_from_file_location('irispark_udc', '{ep}')\n"
            "    _mod = _u.module_from_spec(_spec)\n"
            "    _spec.loader.exec_module(_mod)\n"
            "    args = [a for a in [a1, a2, a3, a4, a5, a6, a7] if a != \"\"]\n"
            "    return _mod.printf(fmt, *args)\n"
            "}\n"
        ),
        (
            "CREATE OR REPLACE FUNCTION irispark_udc_parse_url(url VARCHAR(4000), part VARCHAR(40))\n"
            "RETURNS VARCHAR(4000)\n"
            "LANGUAGE PYTHON\n"
            "{\n"
            "    if url == \"\" or part == \"\":\n"
            "        return None\n"
            "    import importlib.util as _u\n"
            f"    _spec = _u.spec_from_file_location('irispark_udc', '{ep}')\n"
            "    _mod = _u.module_from_spec(_spec)\n"
            "    _spec.loader.exec_module(_mod)\n"
            "    return _mod.parse_url(url, part)\n"
            "}\n"
        ),
        (
            "CREATE OR REPLACE FUNCTION irispark_udc_parse_url_key(\n"
            "    url VARCHAR(4000), part VARCHAR(40), key VARCHAR(40)\n"
            ")\n"
            "RETURNS VARCHAR(4000)\n"
            "LANGUAGE PYTHON\n"
            "{\n"
            "    if url == \"\" or part == \"\" or key == \"\":\n"
            "        return None\n"
            "    import importlib.util as _u\n"
            f"    _spec = _u.spec_from_file_location('irispark_udc', '{ep}')\n"
            "    _mod = _u.module_from_spec(_spec)\n"
            "    _spec.loader.exec_module(_mod)\n"
            "    return _mod.parse_url(url, part, key)\n"
            "}\n"
        ),
        (
            "CREATE OR REPLACE FUNCTION irispark_udc_from_utc_timestamp(ts VARCHAR(40), tz VARCHAR(40))\n"
            "RETURNS VARCHAR(40)\n"
            "LANGUAGE PYTHON\n"
            "{\n"
            "    if ts == \"\" or tz == \"\":\n"
            "        return None\n"
            "    import importlib.util as _u\n"
            f"    _spec = _u.spec_from_file_location('irispark_udc', '{ep}')\n"
            "    _mod = _u.module_from_spec(_spec)\n"
            "    _spec.loader.exec_module(_mod)\n"
            "    return _mod.from_utc_timestamp(ts, tz)\n"
            "}\n"
        ),
        (
            "CREATE OR REPLACE FUNCTION irispark_udc_to_utc_timestamp(ts VARCHAR(40), tz VARCHAR(40))\n"
            "RETURNS VARCHAR(40)\n"
            "LANGUAGE PYTHON\n"
            "{\n"
            "    if ts == \"\" or tz == \"\":\n"
            "        return None\n"
            "    import importlib.util as _u\n"
            f"    _spec = _u.spec_from_file_location('irispark_udc', '{ep}')\n"
            "    _mod = _u.module_from_spec(_spec)\n"
            "    _spec.loader.exec_module(_mod)\n"
            "    return _mod.to_utc_timestamp(ts, tz)\n"
            "}\n"
        ),
    ]


def install(session, udc_path: str | None = None) -> None:
    """Register all EPython UDFs in IRIS.

    *udc_path* defaults to ``IRISPARK_UDC_PATH`` env var, then an
    auto-probed candidate path on the IRIS side.  Warns and skips
    installation when the file is unreachable.
    """
    path: str | None
    if udc_path is not None:
        path = udc_path
    else:
        path = _probe_udc_path(session)
    if path is None:
        warnings.warn(
            "irispark_udc.py not found on IRIS side; skipping EPython UDFs. "
            "Set IRISPARK_UDC_PATH to the server-side absolute path.",
            stacklevel=2,
        )
        return
    for stmt in _build_ddl(path):
        session.sql(stmt)
