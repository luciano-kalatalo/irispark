#!/usr/bin/env python3
"""IrisPark doctor — deployment diagnostic utility (§48).

Usage:
    irispark doctor [--host HOST] [--port PORT] [--namespace NS]
                    [--username USER] [--password PASS]

Checks:
- IRIS connection, version
- Python version, IrisPark version
- CPU flags (AVX, AVX2, BMI, BMI2)
- Columnar / vector support (best-effort)
"""
from __future__ import annotations

import argparse
import platform
import subprocess
import sys
from typing import Any


def _check_iris_connection(host: str, port: int, namespace: str, username: str, password: str) -> tuple[str, str]:
    """Try to connect and return (PASS/FAIL, version_or_message)."""
    try:
        import iris as _iris
        conn = _iris.connect(host, port, namespace, username, password)
        cursor = conn.cursor()
        cursor.execute("SELECT ##class(%SYS.Version).GetNumber()")
        version = str(cursor.fetchone()[0])
        cursor.close()
        conn.close()
        return "PASS", version
    except Exception as exc:
        return "FAIL", str(exc)


def _check_python() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"


def _check_irispark_version() -> str:
    try:
        import importlib.metadata as _meta
        return _meta.version("irispark")
    except Exception:
        return "unknown"


def _check_cpu_flags() -> dict[str, str]:
    """Return CPU flag status as PASS/FAIL/UNKNOWN."""
    flags: dict[str, str] = {"AVX": "UNKNOWN", "AVX2": "UNKNOWN", "BMI": "UNKNOWN", "BMI2": "UNKNOWN"}
    try:
        if platform.system() == "Darwin":
            # macOS: sysctl for ARM/x86 features
            out = subprocess.check_output(["sysctl", "-a"], text=True, stderr=subprocess.DEVNULL)
            # x86 macs may have these; ARM macs don't
            for line in out.splitlines():
                if "hw.optional.avx" in line.lower():
                    flags["AVX"] = "PASS" if "1" in line else "FAIL"
                if "hw.optional.avx2" in line.lower():
                    flags["AVX2"] = "PASS" if "1" in line else "FAIL"
            # On ARM Macs the sysctl keys don't exist; leave UNKNOWN
        elif platform.system() == "Linux":
            with open("/proc/cpuinfo") as fh:
                cpuinfo = fh.read()
            for flag in flags:
                if flag.lower() in cpuinfo.lower():
                    flags[flag] = "PASS"
                else:
                    flags[flag] = "FAIL"
        elif platform.system() == "Windows":
            # No simple portable check; leave UNKNOWN
            pass
    except Exception:
        pass
    return flags


def _check_columnar_vector() -> tuple[str, str]:
    """Best-effort columnar / vector availability."""
    return "UNKNOWN", "UNKNOWN"


def _banner(label: str, width: int = 40) -> str:
    return f"{label.ljust(width)}"


def run(args: Any | None = None) -> int:
    parser = argparse.ArgumentParser(description="IrisPark deployment diagnostic")
    parser.add_argument("--host", default="localhost", help="IRIS host")
    parser.add_argument("--port", type=int, default=1972, help="IRIS port")
    parser.add_argument("--namespace", default="DATASPARK", help="IRIS namespace")
    parser.add_argument("--username", default="suser", help="IRIS username")
    parser.add_argument("--password", default="pass123", help="IRIS password")
    parsed = parser.parse_args(args)

    print("IRISpark Environment Check")
    print("=" * 40)
    print()

    # IRIS connection
    conn_status, conn_msg = _check_iris_connection(
        parsed.host, parsed.port, parsed.namespace, parsed.username, parsed.password
    )
    print(_banner("IRIS connection"), conn_status)
    if conn_status == "PASS":
        print(_banner("IRIS version"), conn_msg)
    else:
        print(_banner("IRIS version"), "N/A")
    print()

    # Python
    print(_banner("Python"), _check_python())
    print(_banner("IrisPark version"), _check_irispark_version())
    print()

    # Platform
    print(_banner("Platform architecture"), platform.machine())
    print()

    # CPU
    print("CPU compatibility:")
    flags = _check_cpu_flags()
    for name in ("AVX", "AVX2", "BMI", "BMI2"):
        print(f"  {name.ljust(20)} {flags[name]}")
    print()

    # Columnar / Vector
    col, vec = _check_columnar_vector()
    print(_banner("Columnar support"), col)
    print(_banner("Vector support"), vec)
    print()

    # Final verdict
    if conn_status == "PASS":
        print("Environment                READY")
    else:
        print("Environment                CHECK FAILED (see IRIS connection)")
    return 0 if conn_status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(run())
