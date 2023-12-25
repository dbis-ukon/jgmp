
from __future__ import annotations
from abc import abstractmethod

from schema.schema_edge import SchemaEdge
from typing import List
from enum import Enum


class EdgeDirection(Enum):
    NATURAL = 1
    REVERSED = 2
    UNDIRECTED = 3

    @staticmethod
    def reverse(direction: EdgeDirection) -> EdgeDirection:
        if direction == EdgeDirection.NATURAL:
            return EdgeDirection.REVERSED
        elif direction == EdgeDirection.REVERSED:
            return EdgeDirection.NATURAL
        else:
            return direction

    @staticmethod
    def string(direction: EdgeDirection) -> str:
        if direction == EdgeDirection.NATURAL:
            return "natural"
        elif direction == EdgeDirection.REVERSED:
            return "reversed"
        elif direction == EdgeDirection.UNDIRECTED:
            return "undirected"
        else:
            raise NotImplementedError()

    @staticmethod
    def from_string(direction: str) -> EdgeDirection:
        if direction == "natural":
            return EdgeDirection.NATURAL
        elif direction == "reversed":
            return EdgeDirection.REVERSED
        elif direction == "undirected":
            return EdgeDirection.UNDIRECTED
        else:
            raise NotImplementedError()


class QueryEdge:
    def __init__(self, labels: List[SchemaEdge], direction: EdgeDirection) -> None:
        self._labels = labels
        self._direction = direction

    def labels(self) -> List[SchemaEdge]:
        return self._labels

    def direction(self) -> EdgeDirection:
        return self._direction

    @abstractmethod
    def copy(self) -> QueryEdge:
        pass
