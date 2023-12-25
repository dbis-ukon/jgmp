

from typing import Optional
from query.graphlike_query import GraphlikeQuery
from query.predicate import Predicate
from query.sql.sql_table_instance import SQLTableInstance
from query.symmetry.cardinality_relation import CardinalityRelation, RelationType
from query.symmetry.generator.cardinality_relation_generator import CardinalityRelationGenerator
import random
from schema.comparison_operator import OPERATORS
from schema.graphlike_schema import GraphlikeSchema


class CardRelComplementarity(CardinalityRelationGenerator):
    def __init__(self, schema: GraphlikeSchema) -> None:
        super().__init__(schema)

    def generate(self, query: GraphlikeQuery) -> Optional[CardinalityRelation]:
        complement_query = query.shallow_copy()
        null_query = query.shallow_copy()
        without_query = query.shallow_copy()
        predicatables = list(query.aliases()).copy()
        random.shuffle(predicatables)
        for predicatable in predicatables:
            if len(predicatable.predicates()) > 0:
                predicates = predicatable.predicates().copy()
                random.shuffle(predicates)
                compliment_predicates = [disjunction for disjunction in predicates[:-1]]
                without_predicates = [disjunction for disjunction in predicates[:-1]]
                null_predicates = [disjunction for disjunction in predicates[:-1]]
                null_attributes = []
                not_null_attributes = []
                for disjunction in null_predicates:
                    if len(disjunction) == 1 and isinstance(disjunction[0], Predicate) and disjunction[0].operator() == OPERATORS["IS"] and disjunction[0].value is None:
                        if disjunction[0].positive():
                            null_attributes.append(disjunction[0].attribute())
                        else:
                            not_null_attributes.append(disjunction[0].attribute())
                null_disjunction_attributes = set()
                for predicate in predicates[-1]:
                    compliment_predicates.append([predicate.complement()])
                    if isinstance(predicate, Predicate) and predicate.operator() != OPERATORS["IS"]:
                        null_disjunction_attributes.add(predicate.attribute())
                null_disjunction = []
                for attribute in null_disjunction_attributes:
                    if attribute in null_attributes:
                        return None
                    if attribute not in not_null_attributes and attribute.nullable():
                        null_disjunction.append(Predicate(attribute, OPERATORS["IS"], None))
                if isinstance(predicatable, SQLTableInstance):
                    complement_predicatable = SQLTableInstance.build(self._schema, predicatable.table(), compliment_predicates, alias=complement_query.alias(predicatable))
                    complement_query.replace_node(predicatable, complement_predicatable)
                    without_predicatable = SQLTableInstance.build(self._schema, predicatable.table(), without_predicates, alias=without_query.alias(predicatable))
                    without_query.replace_node(predicatable, without_predicatable)
                else:
                    raise NotImplementedError()
                left_queries = [query, complement_query]
                if len(null_disjunction) > 0:
                    null_predicates.append(null_disjunction)
                    if isinstance(predicatable, SQLTableInstance):
                        null_predicatable = SQLTableInstance.build(self._schema, predicatable.table(), null_predicates, alias=null_query.alias(predicatable))
                        null_query.replace_node(predicatable, null_predicatable)
                    else:
                        raise NotImplementedError()
                    left_queries.append(null_query)
                return CardinalityRelation(left_queries, [without_query], RelationType.EQUAL)
        return None
