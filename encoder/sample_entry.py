
from __future__ import annotations
from typing import Any, Dict, List, Set

from query.predicatable import Predicatable
from query.predicate import ArbitraryPredicate
from query.query_node import QueryNode
from schema.attribute import Attribute
from schema.schema_node import SchemaNode


class SampleEntry:
    def __init__(self, labels: List[SchemaNode], values: Dict[Attribute, Any]) -> None:
        self._labels = set(labels)
        self._values = values

    def labels(self) -> Set[SchemaNode]:
        return self._labels

    def value(self, attribute: Attribute):
        assert(attribute in self._values)
        return self._values[attribute]

    def evaluate_sample(self, node: QueryNode) -> bool:
        return self.evaluate_sample_labels(node) and (not isinstance(node, Predicatable) or self.evaluate_sample_predicates(node))

    def evaluate_sample_labels(self, node: QueryNode) -> bool:
        for label in node.labels():
            if label not in self._labels:
                return False
        return True

    def evaluate_sample_predicates(self, predicatable: Predicatable) -> bool:
        predicates = predicatable.predicates()
        for disjunction in predicates:
            dis_result = False
            for predicate in disjunction:
                if isinstance(predicate, ArbitraryPredicate):
                    raise NotImplementedError
                if predicate.operator().compare(self._values[predicate.attribute()], predicate.value()) == predicate.positive():
                    dis_result = True
                    break
            if not dis_result:
                return False
        return True

    def values_equal(self, other: SampleEntry, attributes: List[Attribute]) -> bool:
        for attribute in attributes:
            if not self.value(attribute) == other.value(attribute):
                return False
        return True
