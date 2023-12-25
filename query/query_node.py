
from __future__ import annotations
from abc import abstractmethod
from typing import List
from schema.schema_node import SchemaNode


class QueryNode:
    def __init__(self, labels: List[SchemaNode], cardinality: float, virtual: bool = False) -> None:
        self._labels = labels
        self._cardinality = cardinality
        self._virtual = virtual

    def labels(self) -> List[SchemaNode]:
        return self._labels

    def cardinality(self) -> float:
        return self._cardinality

    def virtual(self) -> bool:
        return self._virtual

    def set_cardinality(self, cardinality: float):
        self._cardinality = cardinality

    @abstractmethod
    def copy(self) -> QueryNode:
        pass
