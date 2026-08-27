from __future__ import annotations

from typing import Any


class LogicalVector:
    """
    Logical representation of a feature vector.

    Rather than immediately materializing a dense or sparse vector,
    LogicalVector maintains the metadata about which columns compose
    the feature vector and their roles. The ML Semantic Planner can
    then decide how to materialize it for the chosen backend.
    """

    def __init__(
        self,
        columns: list[str],
        vectorType: str = "dense",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Args:
            columns: List of column names that make up this feature vector.
            vectorType: "dense" or "sparse" - logical representation type.
            metadata: Additional metadata about the vector.
        """
        self.columns = columns
        self.vectorType = vectorType
        self.metadata = metadata or {}

    @property
    def size(self) -> int:
        """Number of columns in the vector."""
        return len(self.columns)

    def __repr__(self) -> str:
        return f"LogicalVector(columns={self.columns}, vectorType={self.vectorType})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, LogicalVector):
            return False
        return (
            self.columns == other.columns
            and self.vectorType == other.vectorType
            and self.metadata == other.metadata
        )

    def __hash__(self) -> int:
        return hash((tuple(self.columns), self.vectorType, frozenset(self.metadata.items())))


def resolve_features(featuresCol):
    """Return the feature column list from either a LogicalVector or a list.

    Accepts ``LogicalVector`` (uses its ``columns``) or a plain list of column
    names, so estimators can take either form.
    """
    if isinstance(featuresCol, LogicalVector):
        return list(featuresCol.columns)
    return list(featuresCol)
