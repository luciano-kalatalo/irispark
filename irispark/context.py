from __future__ import annotations

from typing import Any

from .accumulator import Accumulator
from .rdd import RDD


class IrisSparkContext:
    def __init__(self, session: Any) -> None:
        self._session = session

    def parallelize(self, data: list[Any], numSlices: int = 1) -> RDD:
        data = list(data)
        if not data:
            return RDD([[]], self)
        if numSlices < 1:
            numSlices = 1
        if numSlices > len(data):
            numSlices = len(data)
        chunk_size = max(1, len(data) // numSlices)
        partitions = []
        for i in range(0, len(data), chunk_size):
            partitions.append(data[i:i + chunk_size])
        if len(partitions) != numSlices and len(data) > 1:
            while len(partitions) > numSlices:
                if len(partitions) >= 2:
                    partitions[-2].extend(partitions[-1])
                    partitions.pop()
                else:
                    break
        return RDD(partitions, self)

    def textFile(self, path: str, minPartitions: int | None = None) -> RDD:
        with open(path) as f:
            lines = [line.rstrip("\n") for line in f]
        num_parts = minPartitions or 1
        return self.parallelize(lines, num_parts)

    def broadcast(self, value: Any) -> SimpleBroadcast:
        return SimpleBroadcast(value)

    def accumulator(self, initial_value: int = 0) -> Accumulator:
        return Accumulator(initial_value)


class SimpleBroadcast:
    def __init__(self, value: Any) -> None:
        self._value = value

    @property
    def value(self) -> Any:
        return self._value

    def unpersist(self) -> None:
        pass

    def destroy(self) -> None:
        pass

    def __repr__(self) -> str:
        return f"Broadcast(value={self._value!r})"
