

from typing import Optional
from query.graphlike_query import GraphlikeQuery
from query.predicate import Predicate
from query.sql.sql_table_instance import SQLTableInstance
from query.symmetry.cardinality_relation import CardinalityRelation, RelationType
from query.symmetry.generator.cardinality_relation_generator import CardinalityRelationGenerator
import random

from schema.comparison_operator import OPERATORS


class CardRelInclusionExclusion(CardinalityRelationGenerator):
    def generate(self, query: GraphlikeQuery) -> Optional[CardinalityRelation]:
        a_query = query.shallow_copy()
        b_query = query.shallow_copy()
        predicatables = list(query.aliases()).copy()
        random.shuffle(predicatables)
        modified = False
        for predicatable in predicatables:
            if not any(len(disjunction) > 1 for disjunction in predicatable.predicates()):
                continue
            disjunctions = predicatable.predicates().copy()
            random.shuffle(disjunctions)
            a_disjunction = []
            b_disjunction = []
            other_disjunctions = []
            for disjunction in disjunctions:
                if len(disjunction) > 1 and len(a_disjunction) == 0:
                    copied_disjunction = disjunction.copy()
                    random.shuffle(disjunction)
                    split = random.randrange(1, len(disjunction))
                    a_disjunction = copied_disjunction[:split]
                    b_disjunction = copied_disjunction[split:]
                    modified = True
                else:
                    other_disjunctions.append(disjunction)
            if modified:
                left_queries = [query]
                if isinstance(predicatable, SQLTableInstance):
                    a_predicatable = SQLTableInstance.build(self._schema, predicatable.table(), other_disjunctions + [a_disjunction], alias=a_query.alias(predicatable))
                    a_query.replace_node(predicatable, a_predicatable)
                    b_predicatable = SQLTableInstance.build(self._schema, predicatable.table(), other_disjunctions + [b_disjunction], alias=b_query.alias(predicatable))
                    b_query.replace_node(predicatable, b_predicatable)
                    simple =  all(isinstance(predicate, Predicate) for predicate in a_disjunction + b_disjunction)
                    if simple:
                        positive_equal = all(predicate.operator() == OPERATORS["="] and predicate.positive() for predicate in a_disjunction + b_disjunction)
                        disjoint = {predicate.value() for predicate in a_disjunction}.isdisjoint({predicate.value() for predicate in b_disjunction})
                    if not simple or not (positive_equal and disjoint):
                        ab_query = query.shallow_copy()
                        ab_predicatable = SQLTableInstance.build(self._schema, predicatable.table(), other_disjunctions + [a_disjunction, b_disjunction])
                        ab_query.replace_node(predicatable, ab_predicatable)
                        left_queries.append(ab_query)
                else:
                    raise NotImplementedError()
                return CardinalityRelation(left_queries, [a_query, b_query], RelationType.EQUAL)
        return None

