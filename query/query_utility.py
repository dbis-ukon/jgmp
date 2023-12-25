
from query.predicate import Predicate, ArbitraryPredicate
from typing import List, Union


def predicates_to_string(alias: str, predicates: List[List[Union[Predicate, ArbitraryPredicate]]]) -> str:
    conjunction_strings = []
    for predicate_group in predicates:
        disjunction_strings = []
        for predicate in predicate_group:
            disjunction_strings.append(predicate.query_string(alias))
        disjunction_string = " OR ".join(disjunction_strings)
        if len(disjunction_strings) > 1:
            disjunction_string = "(%s)" % disjunction_string
        conjunction_strings.append(disjunction_string)
    return "\n\tAND ".join(conjunction_strings)
