from __future__ import annotations

from enum import Enum


class BackendType(Enum):
    """Supported execution backends."""
    SQL = "sql"
    INTEGRATEDML = "integratedml"
    PYTHON = "python"
    AUTO = "auto"


class BackendCapability:
    """Describes what a backend can do."""

    def __init__(
        self,
        name: str,
        supported_estimators: list[str] | None = None,
        supported_transformers: list[str] | None = None,
        supported_operations: list[str] | None = None,
    ) -> None:
        self.name = name
        self.supported_estimators = supported_estimators or []
        self.supported_transformers = supported_transformers or []
        self.supported_operations = supported_operations or []


class MLSemanticPlanner:
    """
    The ML Semantic Planner is the central component that determines
    the optimal execution backend for each ML operation.

    Responsibilities:
    - Validate estimator and transformer parameters
    - Validate input schemas
    - Resolve feature columns
    - Manage logical vector metadata
    - Analyze pipeline dependencies
    - Determine execution backend
    - Generate SQL expressions
    - Generate training operations
    - Manage temporary artifacts
    * Map Spark semantics to IRIS capabilities
    * Preserve feature lineage
    * Manage prediction columns
    * Coordinate model persistence
    """

    def __init__(self) -> None:
        self._backends: dict[str, dict] = {}
        self._default_backend: str = "sql"
        self._register_default_backends()

    def _register_default_backends(self) -> None:
        """Register the default backend capabilities."""
        # Python backend (numpy fit for the supervised estimators) is checked
        # first so LinearRegression/LogisticRegression resolve to "python".
        self._backends["python"] = {
            "name": "python",
            "type": "python",
            "supported_estimators": [
                "LinearRegression", "LogisticRegression",
            ],
            "supported_transformers": [],
            "operations": ["fit", "transform", "predict"],
        }

        # Embedded Python (sklearn) backend - tree/KNN ensembles fit server-side.
        self._backends["embedded_python"] = {
            "name": "embedded_python",
            "type": "python",
            "supported_estimators": [
                "RandomForestClassifier", "RandomForestRegressor",
                "KNeighborsClassifier", "KNeighborsRegressor",
            ],
            "supported_transformers": [],
            "operations": ["fit", "transform", "predict"],
        }

        # SQL backend - feature transformers and SQL-pushdown prediction
        self._backends["sql"] = {
            "name": "sql",
            "type": "sql",
            "supported_transformers": [
                "VectorAssembler", "StringIndexer", "OneHotEncoder",
                "StandardScaler", "QuantileDiscretizer", "Binarizer",
                "Imputer", "MinMaxScaler", "MaxAbsScaler",
                "IndexToString", "SQLTransformer",
            ],
            "supported_estimators": [],
            "operations": ["transform", "fit", "predict"],
        }

        # IntegratedML backend
        self._backends["integratedml"] = {
            "name": "integratedml",
            "type": "integratedml",
            "supported_estimators": [
                "AutoMLClassifier", "AutoMLRegressor",
            ],
            "supported_transformers": [],
            "operations": ["fit", "transform", "predict"],
        }

    def get_backend(self, name: str) -> dict | None:
        """Get backend by name."""
        return self._backends.get(name)

    def list_backends(self) -> list[str]:
        """List available backends."""
        return list(self._backends.keys())

    def get_default_backend(self) -> str:
        """Get the default backend."""
        return self._default_backend

    def set_default_backend(self, name: str) -> None:
        """Set the default backend."""
        if name not in self._backends:
            raise ValueError(f"Unknown backend: {name}")
        self._default_backend = name

    def resolve_backend(
        self,
        estimator_or_transformer: object,
        preferred: str | None = None,
    ) -> str:
        """
        Determines the optimal execution backend for an estimator or transformer.

        Args:
            estimator_or_transformer: The estimator or transformer to resolve.
            preferred: Optional preferred backend name.

        Returns:
            The name of the selected backend.
        """
        # If a preferred backend is specified and available, use it
        if preferred and preferred in self._backends:
            return preferred

        # Get the class name
        class_name = estimator_or_transformer.__class__.__name__

        # Check each backend for support
        for backend_name, backend_info in self._backends.items():
            if class_name in backend_info.get("supported_transformers", []):
                return backend_name
            if class_name in backend_info.get("supported_estimators", []):
                return backend_name

        # Fall back to default
        return self._default_backend

    def get_backend_capabilities(self, name: str) -> dict | None:
        """Get the capabilities of a backend."""
        return self._backends.get(name)

    def register_backend(self, name: str, capabilities: dict) -> None:
        """Register a new backend."""
        self._backends[name] = capabilities

    def can_execute(self, name: str, operation: str) -> bool:
        """Check if a backend can execute an operation."""
        backend = self._backends.get(name)
        if not backend:
            return False
        return operation in backend.get("operations", [])

    def list_estimators_for_backend(self, backend_name: str) -> list[str]:
        """List estimators supported by a backend."""
        backend = self._backends.get(backend_name)
        if not backend:
            return []
        return backend.get("supported_estimators", [])

    def list_transformers_for_backend(self, backend_name: str) -> list[str]:
        """List transformers supported by a backend."""
        backend = self._backends.get(backend_name)
        if not backend:
            return []
        return backend.get("supported_transformers", [])

# Module-level singleton consulted by Transformer/Estimator for backend
# metadata (ml_scope §41). Inject/replace to customize routing.
default_planner = MLSemanticPlanner()
