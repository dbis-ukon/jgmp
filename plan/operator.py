

from __future__ import annotations
import abc
from plan.property import Property
from query.graphlike_query import GraphlikeQuery
from query.query_edge import QueryEdge
from query.query_node import QueryNode
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple


class Operator:
    def __init__(self, name: str, arity: int, represented_nodes: List[QueryNode], represented_edges: List[Tuple[QueryNode, QueryEdge, QueryNode]]) -> None:
        self._name = name
        self._arity = arity
        self._represented_nodes = represented_nodes
        self._represented_edges = represented_edges

    def name(self) -> str:
        return self._name

    def arity(self) -> int:
        return self._arity

    def represented_nodes(self) -> List[QueryNode]:
        return self._represented_nodes

    def represented_edges(self) -> List[Tuple[QueryNode, QueryEdge, QueryNode]]:
        return self._represented_edges

    def string(self, alias: Dict[Any, str] = {}) -> str:
        return self._name

    @abc.abstractmethod
    def requires(self,
                 required: Set[Property],
                 children: List[GraphlikeQuery]) -> Optional[List[FrozenSet[Property]]]:
        pass

    def equal(self, other: Operator):
        # TODO: implement checks for specific operators
        return self._name == other.name() and self.represented_nodes() == other.represented_nodes() and self.represented_edges() == other.represented_edges()
