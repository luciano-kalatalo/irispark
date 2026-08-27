from __future__ import annotations

import random
from collections.abc import Callable
from functools import reduce as _reduce
from typing import Any


class RDD:
    def __init__(self, partitions: list[list[Any]], ctx: Any = None) -> None:
        self._partitions: list[list[Any]] = [list(p) if p else [] for p in partitions]
        if not self._partitions:
            self._partitions = [[]]
        self._pipeline: list[Callable[[list[list[Any]]], list[list[Any]]]] = []
        self._ctx = ctx
        self._cached: list[Any] | None = None
        self._name: str | None = None

    def _copy(self) -> RDD:
        rdd = RDD(list(self._partitions), self._ctx)
        rdd._pipeline = list(self._pipeline)
        rdd._name = self._name
        return rdd

    def _compute(self) -> list[list[Any]]:
        if self._cached is not None:
            return [[x] for x in self._cached]
        data = self._partitions
        for fn in self._pipeline:
            data = fn(data)
        return data

    def _compute_flat(self) -> list[Any]:
        if self._cached is not None:
            return list(self._cached)
        return [x for p in self._compute() for x in p]

    def map(self, fn: Callable[[Any], Any]) -> RDD:
        def _map(partitions: list[list[Any]]) -> list[list[Any]]:
            return [[fn(x) for x in p] for p in partitions]

        result = self._copy()
        result._pipeline.append(_map)
        return result

    def filter(self, fn: Callable[[Any], bool]) -> RDD:
        def _filter(partitions: list[list[Any]]) -> list[list[Any]]:
            return [[x for x in p if fn(x)] for p in partitions]

        result = self._copy()
        result._pipeline.append(_filter)
        return result

    def flatMap(self, fn: Callable[[Any], list[Any]]) -> RDD:
        def _flatMap(partitions: list[list[Any]]) -> list[list[Any]]:
            return [[y for x in p for y in fn(x)] for p in partitions]

        result = self._copy()
        result._pipeline.append(_flatMap)
        return result

    def sample(self, withReplacement: bool, fraction: float, seed: int | None = None) -> RDD:
        rng = random.Random(seed)

        def _sample(partitions: list[list[Any]]) -> list[list[Any]]:
            flat = [x for p in partitions for x in p]
            if len(flat) == 0:
                return [[]]
            n = int(len(flat) * fraction)
            if n == 0:
                return [[]]
            if withReplacement:
                return [[rng.choice(flat) for _ in range(n)]]
            return [list(rng.sample(flat, min(n, len(flat))))]

        result = self._copy()
        result._pipeline.append(_sample)
        return result

    def union(self, other: RDD) -> RDD:
        def _union(partitions: list[list[Any]]) -> list[list[Any]]:
            return partitions + list(other._compute())

        result = self._copy()
        result._pipeline.append(_union)
        return result

    def intersection(self, other: RDD) -> RDD:
        def _inter(partitions: list[list[Any]]) -> list[list[Any]]:
            flat1 = {x for p in partitions for x in p}
            flat2 = {x for p in other._compute() for x in p}
            return [[x for x in flat1 if x in flat2]]

        result = self._copy()
        result._pipeline.append(_inter)
        return result

    def distinct(self) -> RDD:
        def _distinct(partitions: list[list[Any]]) -> list[list[Any]]:
            seen: set[Any] = set()
            result: list[Any] = []
            for p in partitions:
                for x in p:
                    if x not in seen:
                        seen.add(x)
                        result.append(x)
            return [result]

        result = self._copy()
        result._pipeline.append(_distinct)
        return result

    def glom(self) -> RDD:
        def _glom(partitions: list[list[Any]]) -> list[list[Any]]:
            return [[list(p)] for p in partitions]

        result = self._copy()
        result._pipeline.append(_glom)
        return result

    def sortBy(self, keyfunc: Callable[[Any], Any], ascending: bool = True) -> RDD:
        def _sort(partitions: list[list[Any]]) -> list[list[Any]]:
            flat = [x for p in partitions for x in p]
            flat.sort(key=keyfunc, reverse=not ascending)
            return [flat]

        result = self._copy()
        result._pipeline.append(_sort)
        return result

    def collect(self) -> list[Any]:
        return self._compute_flat()

    def count(self) -> int:
        return len(self._compute_flat())

    def first(self) -> Any:
        for p in self._compute():
            if p:
                return p[0]
        raise IndexError("empty RDD")

    def take(self, n: int) -> list[Any]:
        result: list[Any] = []
        for p in self._compute():
            for x in p:
                result.append(x)
                if len(result) >= n:
                    return result
        return result

    def takeSample(self, withReplacement: bool, num: int, seed: int | None = None) -> list[Any]:
        rng = random.Random(seed)
        flat = self._compute_flat()
        if not flat:
            return []
        if withReplacement:
            return [rng.choice(flat) for _ in range(num)]
        return rng.sample(flat, min(num, len(flat)))

    def takeOrdered(self, n: int, key: Callable[[Any], Any] | None = None, ascending: bool = True) -> list[Any]:
        flat = self._compute_flat()
        flat.sort(key=key, reverse=not ascending)
        return flat[:n]

    def reduce(self, fn: Callable[[Any, Any], Any]) -> Any:
        flat = self._compute_flat()
        if not flat:
            raise ValueError("Cannot reduce empty RDD")
        return _reduce(fn, flat)

    def foreach(self, fn: Callable[[Any], Any]) -> None:
        for x in self._compute_flat():
            fn(x)

    def isEmpty(self) -> bool:
        for p in self._compute():
            if p:
                return False
        return True

    def cache(self) -> RDD:
        self._cached = self._compute_flat()
        return self

    def persist(self) -> RDD:
        return self.cache()

    def unpersist(self) -> RDD:
        self._cached = None
        return self

    def toDF(self, schema: Any = None) -> Any:
        if self._ctx is None or self._ctx._session is None:
            raise RuntimeError(
                "Cannot convert to DataFrame without an active session"
            )

        data = self._compute_flat()

        if schema is None:
            if data and isinstance(data[0], dict):
                schema = list(data[0].keys())
                data = [tuple(d[k] for k in schema) for d in data]
            elif data and isinstance(data[0], (tuple, list)):
                schema = [f"col_{i}" for i in range(len(data[0]))]
            else:
                schema = ["value"]
                data = [(x,) for x in data]
        else:
            if data and not isinstance(data[0], (tuple, list, dict)):
                data = [(x,) for x in data]

        return self._ctx._session.createDataFrame(data, schema)

    def name(self) -> str | None:
        return self._name

    def setName(self, name: str) -> RDD:
        self._name = name
        return self

    def __repr__(self) -> str:
        name = f"#{self._name}" if self._name else ""
        return f"RDD{name}[{len(self._partitions)} partitions]"
