

from __future__ import annotations
from abc import abstractmethod


class Property:
    def __init__(self, name: str) -> None:
        self._name = name

    @abstractmethod
    def includes(self, other: Property) -> bool:
        pass

    def __eq__(self, o: Property) -> bool:
        return self.includes(o) and o.includes(self)
