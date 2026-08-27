from __future__ import annotations


class Accumulator:
    def __init__(self, initial_value: int = 0) -> None:
        self._value = initial_value

    def add(self, value: int) -> None:
        self._value += value

    @property
    def value(self) -> int:
        return self._value

    def __iadd__(self, value: int) -> Accumulator:
        self.add(value)
        return self

    def __repr__(self) -> str:
        return f"Accumulator<value={self._value}>"
