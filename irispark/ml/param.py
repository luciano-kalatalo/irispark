from __future__ import annotations

from collections.abc import Callable
from typing import Any, Generic, TypeVar

from typing_extensions import Self


class TypeConverters:
    """Type converters for Param validation."""

    @staticmethod
    def identity(x: Any) -> Any:
        return x

    @staticmethod
    def toString(x: Any) -> str:
        return str(x)

    @staticmethod
    def toInt(x: Any) -> int:
        return int(x)

    @staticmethod
    def toFloat(x: Any) -> float:
        return float(x)

    @staticmethod
    def toBoolean(x: Any) -> bool:
        if isinstance(x, str):
            return x.lower() in ("true", "1", "yes")
        return bool(x)

    @staticmethod
    def toList(x: Any) -> list:
        if isinstance(x, list):
            return x
        if isinstance(x, tuple):
            return list(x)
        if x is None:
            return []
        return [x]

    @staticmethod
    def toListString(x: Any) -> list[str]:
        if isinstance(x, list):
            return [str(v) for v in x]
        if isinstance(x, str):
            return [v.strip() for v in x.split(",")]
        return [str(x)]


T = TypeVar("T")


class Param(Generic[T]):
    """A param with self-contained documentation."""

    def __init__(
        self,
        parent: Params,
        name: str,
        doc: str,
        typeConverter: Callable[[Any], T] = TypeConverters.identity,
        defaultValue: T | None = None,
    ) -> None:
        self.parent = parent
        self.name = name
        self.doc = doc
        self.typeConverter = typeConverter
        self.defaultValue = defaultValue
        self._default = defaultValue

    def __str__(self) -> str:
        return f"Param(name={self.name!r}, doc={self.doc!r})"

    def __repr__(self) -> str:
        return f"Param(name={self.name!r}, doc={self.doc!r})"


class Params:
    """Component that manages params."""

    def __init__(self) -> None:
        self._params: dict[str, Param] = {}
        self._paramValues: dict[str, Any] = {}
        self._paramDocs: dict[str, str] = {}

    def _setParam(self, name: str, value: Any) -> Self:
        """Sets a parameter value."""
        if name not in self._params:
            raise ValueError(f"Param {name!r} does not exist.")
        param = self._params[name]
        converted = param.typeConverter(value)
        self._paramValues[name] = converted
        return self

    def _setDefault(self, name: str, value: Any) -> Self:
        """Sets the default value for a parameter."""
        if name not in self._params:
            raise ValueError(f"Param {name!r} does not exist.")
        param = self._params[name]
        param.defaultValue = param.typeConverter(value)
        param._default = param.defaultValue
        if name not in self._paramValues:
            self._paramValues[name] = param.defaultValue
        return self

    def _set(self, name: str, value: Any) -> Self:
        """Sets a parameter value and marks it as explicitly set."""
        self._setParam(name, value)
        return self

    def _getParam(self, name: str) -> Param:
        """Gets the Param object."""
        if name not in self._params:
            raise ValueError(f"Param {name!r} does not exist.")
        return self._params[name]

    def getParam(self, name: str) -> Param:
        """Gets the Param object (public API)."""
        return self._getParam(name)

    def _getParamValue(self, name: str) -> Any:
        """Gets the value of a parameter."""
        if name not in self._params:
            raise ValueError(f"Param {name!r} does not exist.")
        if name in self._paramValues:
            return self._paramValues[name]
        param = self._params[name]
        if param.defaultValue is not None:
            return param.defaultValue
        raise ValueError(f"No value set for param {name!r}")

    def getOrDefault(self, param: Param | str) -> Any:
        """Gets the value of a param or its default value."""
        name = param.name if isinstance(param, Param) else param
        return self._getParamValue(name)

    def hasParam(self, name: str) -> bool:
        """Checks if a param exists."""
        return name in self._params

    def set(self, name: str, value: Any) -> Params:
        """Sets a parameter value (public API)."""
        return self._setParam(name, value)

    def setParams(self, **kwargs: Any) -> Params:
        """Sets multiple params."""
        for k, v in kwargs.items():
            self._setParam(k, v)
        return self

    def extractParamMap(self) -> dict[str, Any]:
        """Extracts all explicitly set param values."""
        return self._paramValues.copy()

    def copy(self, extra: dict[str, Any] | None = None) -> Params:
        """Creates a copy of this Params instance with optionally extra param values."""
        new = self.__class__()
        new._params = self._params.copy()
        new._paramValues = self._paramValues.copy()
        new._paramDocs = self._paramDocs.copy()
        if extra:
            for k, v in extra.items():
                new._setParam(k, v)
        return new

    def _addParam(self, param: Param) -> Params:
        """Internal method to register a new param."""
        self._params[param.name] = param
        self._paramDocs[param.name] = param.doc
        return self

    def _register(
        self,
        name: str,
        doc: str,
        value: Any,
        typeConverter: Callable[[Any], Any] = TypeConverters.identity,
    ) -> Params:
        """Register a param and set its value in one call.

        Convenience for transformer ``__init__``: declares the param, records
        its default, and stores the provided value so both the Param system
        and backward-compatible attribute access work.
        """
        param = Param(
            parent=self,
            name=name,
            doc=doc,
            typeConverter=typeConverter,
            defaultValue=value,
        )
        self._addParam(param)
        self._paramValues[name] = param.typeConverter(value)
        return self

    def _getParams(self) -> list[Param]:
        """Returns all params."""
        return list(self._params.values())

    def explainParams(self) -> str:
        """Returns a string representation of all params and their values."""
        lines = []
        for param in self._params.values():
            value = self._paramValues.get(param.name, param.defaultValue)
            lines.append(f"{param.name}: {value} (default: {param.defaultValue}) - {param.doc}")
        return "\n".join(lines)

    def __getattr__(self, name: str) -> Any:
        """Backward-compatible attribute access for registered params.

        Lets ``self.inputCol`` resolve to the param value even when the
        transformer stores it only in the Param system. Falls back to
        AttributeError for anything that is not a registered param.
        """
        params = self.__dict__.get("_params", {})
        if name in params:
            return self._getParamValue(name)
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")
