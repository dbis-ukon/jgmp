
from __future__ import annotations
import abc
from query.graphlike_query import GraphlikeQuery
from query.query_edge import QueryEdge
from query.query_node import QueryNode

from typing import Any, Dict, List, Tuple
from plan.operator import Operator


class Expression:
    def __init__(self, operator: Operator, children: List[Expression]) -> None:
        assert(operator.arity() == len(children))
        self._operator = operator
        self._children = children

    def operator(self) -> Operator:
        return self._operator

    def children(self) -> List[Expression]:
        return self._children

    def _expression_represents(self) -> Tuple[List[QueryNode], List[Tuple[QueryNode, QueryEdge, QueryNode]]]:
        represented_nodes = self._operator.represented_nodes().copy()
        represented_edges = self._operator.represented_edges().copy()

        for child in self._children:
            child_nodes, child_edges = child._expression_represents()
            represented_nodes += child_nodes
            represented_edges += child_edges

        return represented_nodes, represented_edges

    @abc.abstractmethod
    def subquery(self) -> GraphlikeQuery:
        pass

    def string(self, alias: Dict[Any, str] = {}) -> str:
        if self._operator.arity() == 0:
            return self._operator.string(alias=alias)
        elif self._operator.arity() == 2:
            return "(%s) %s (%s)" % (self._children[0].string(alias=alias), self._operator.string(), self._children[1].string(alias=alias))
        else:
            return "%s(%s)" % (self._operator.string(alias=alias), ", ".join([child.string(alias=alias) for child in self._children]))

    def json(self, alias: Dict[Any, str] = {}):
        operator_json = self._operator.string(alias=alias)
        children_json = []
        for child in self._children:
            children_json.append(child.json(alias=alias))
        return {"operator": operator_json, "children": children_json}

    def subexpressions(self) -> List[Expression]:
        subexpressions = [self]
        for child in self._children:
            subexpressions += child.subexpressions()
        return subexpressions

    def equal(self, other: Expression) -> bool:
        for child, other_child in zip(self._children, other.children()):
            if not child.equal(other_child):
                return False
        return self._operator.equal(other.operator())
