from __future__ import annotations

import re
from typing import Any

_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str, label: str) -> None:
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid {label} '{name}'. "
            f"Must match pattern: {_SAFE_IDENTIFIER_RE.pattern}"
        )


class Catalog:
    def __init__(self, session: Any) -> None:
        self._session = session

    def _default_schema(self) -> str:
        return "SQLUser"

    def listTables(self, dbName: str | None = None) -> list[str]:
        schema = dbName if dbName else self._default_schema()
        _validate_identifier(schema, "schema name")
        rows, _ = self._session.sql(
            f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{schema}'"
        )
        return [r[0] for r in rows]

    def tableExists(self, tableName: str, dbName: str | None = None) -> bool:
        schema = dbName if dbName else self._default_schema()
        _validate_identifier(schema, "schema name")
        _validate_identifier(tableName, "table name")
        rows, _ = self._session.sql(
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{tableName}'"
        )
        return rows[0][0] > 0 if rows else False
