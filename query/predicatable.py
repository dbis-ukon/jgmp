
from query.query_utility import predicates_to_string
from schema.attributable import Attributable
from query.predicate import Predicate, ArbitraryPredicate
from typing import Any, List, Tuple, Union


class Predicatable:
    def __init__(self, labels: List[Attributable], predicates: List[List[Union[Predicate, ArbitraryPredicate]]]) -> None:
        self._labels = labels
        self._predicates = predicates
        attributes = [attribute for label in labels for attribute in label.attributes()]
        for predicate_group in predicates:
            assert(len(predicate_group) > 0)
            for predicate in predicate_group:
                assert(isinstance(predicate, ArbitraryPredicate) or predicate.attribute() in attributes)

    def labels(self) -> List[Attributable]:
        return self._labels

    def predicates(self) -> List[List[Union[Predicate, ArbitraryPredicate]]]:
        return self._predicates

    def predicate_string(self, alias: str) -> str:
        return predicates_to_string(alias, self._predicates)

    def set_predicates(self, predicates: List[List[Union[Predicate, ArbitraryPredicate]]]):
        self._predicates = predicates

    @staticmethod
    def from_json(json: List[Tuple[str, str, Any]], labels: List[Attributable]) -> List[Predicate]:
        return [[Predicate.from_json(j, labels)] for j in json]

    def hash(self):
        return hash(frozenset([frozenset(p) for p in self._predicates]))
