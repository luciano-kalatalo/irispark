"""Row class for IrisPark — tuple-like with attribute access (§16)."""

from __future__ import annotations

from typing import Any


class Row(tuple):
    """
    A row with both index and attribute access, compatible with tuple semantics.

    Example:
        row = Row(name="Alice", age=30)
        row[0] == row.name == "Alice"
        row[1] == row.age == 30
    """

    def __new__(cls, *args, **kwargs):
        # Internal override: Row(values..., _fields=[names]) attaches field
        # names to positional data instead of treating _fields as a data
        # keyword. Without this branch the positional values were silently
        # discarded whenever any kwarg was present.
        fields_override = kwargs.pop("_fields", None)
        if kwargs:
            # Called as Row(name="Alice", age=30)
            values = tuple(kwargs.values())
            fields = tuple(kwargs.keys())
        elif args and isinstance(args[0], dict):
            # Called as Row({"name": "Alice", "age": 30})
            values = tuple(args[0].values())
            fields = tuple(args[0].keys())
        elif args and isinstance(args[0], (tuple, list)):
            # Called as Row(("Alice", 30)) or Row([("name", "Alice"), ("age", 30)])
            if args[0] and isinstance(args[0][0], (tuple, list)):
                # List of (name, value) pairs
                fields = tuple(item[0] for item in args[0])
                values = tuple(item[1] for item in args[0])
            else:
                # Just values: (value1, value2)
                values = tuple(args[0])
                fields = ()
        else:
            # Called as Row(value1, value2, ...)
            values = args
            fields = ()
        if fields_override is not None:
            fields = tuple(fields_override)
        self = tuple.__new__(cls, values)
        self._fields = fields
        return self

    def __init__(self, *args, **kwargs):
        # tuple is immutable, __init__ does nothing
        pass

    def __getattr__(self, item: str) -> Any:
        """Allow attribute access by field name: row.name"""
        if item in self._fields:
            return self[self._fields.index(item)]
        raise AttributeError(f"'Row' object has no attribute '{item}'")

    def __getitem__(self, item: int | slice | str) -> Any:  # type: ignore[override]  # PySpark parity: row["name"] string access
        """Allow index access: row[0], row["name"]"""
        if isinstance(item, str):
            if item in self._fields:
                return self[self._fields.index(item)]
            raise KeyError(item)
        return tuple.__getitem__(self, item)

    def __contains__(self, item: str) -> bool:  # type: ignore[override]  # PySpark parity: membership by column name
        return item in self._fields

    def __repr__(self) -> str:
        if self._fields:
            parts = [f"{k}={v!r}" for k, v in zip(self._fields, self)]
            return f"Row({', '.join(parts)})"
        return f"Row({', '.join(repr(v) for v in self)})"

    def asDict(self) -> dict[str, Any]:
        """Return row as a dictionary."""
        return dict(zip(self._fields, self))

    def __reduce__(self):
        return (self.__class__, (tuple(self),), {"_fields": self._fields})

    def __setstate__(self, state):
        self._fields = state.get("_fields", ())


def _make_row(row: tuple[Any, ...], columns: list[str]) -> Row:
    """Create a Row from a tuple and column names."""
    if not columns:
        return Row(*row)
    r = Row(*row)
    r._fields = tuple(columns)
    return r
