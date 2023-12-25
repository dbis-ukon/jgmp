

import random
from typing import Optional
from query.graphlike_query import GraphlikeQuery
from query.predicate import Predicate
from query.sql.sql_table_instance import SQLTableInstance
from query.symmetry.cardinality_relation import CardinalityRelation, RelationType
from query.symmetry.generator.cardinality_relation_generator import CardinalityRelationGenerator
from schema.comparison_operator import OPERATORS
from schema.graphlike_schema import GraphlikeSchema
from schema.value_picker import ValuePicker


class CardRelGreaterMonotonicity(CardinalityRelationGenerator):
    def __init__(self, schema: GraphlikeSchema, value_picker: ValuePicker) -> None:
        super().__init__(schema)
        self._value_picker = value_picker

    def generate(self, query: GraphlikeQuery) -> Optional[CardinalityRelation]:
        modified_query = query.shallow_copy()
        predicatables = list(query.aliases()).copy()
        random.shuffle(predicatables)
        modified = False
        for predicatable in predicatables:
            if len(predicatable.predicates()) == 0:
                continue
            copied_predicates = []
            disjunctions = predicatable.predicates().copy()
            random.shuffle(disjunctions)
            for disjunction in disjunctions:
                copied_disjunction = []
                for predicate in disjunction:
                    if not modified and isinstance(predicate, Predicate) and (predicate.operator() in [OPERATORS[">"], OPERATORS["<"]]):
                        labels = predicatable.labels()
                        assert(len(labels) == 1)  # TODO: multiple labels
                        node = labels[0]
                        attribute = predicate.attribute()
                        value = self._value_picker.pick_random(node, attribute)
                        if value == predicate.value():
                            return None
                        modified_greater = 1 if value > predicate.value() else -1
                        positive = 1 if predicate.positive() else -1
                        greater = 1 if predicate.operator() == OPERATORS[">"] else -1
                        monotonicity = modified_greater * positive * greater
                        modified = True
                        copied_disjunction.append(Predicate(predicate.attribute(), predicate.operator(), value, positive=predicate.positive()))
                    else:
                        copied_disjunction.append(predicate)
                copied_predicates.append(copied_disjunction)
            if modified:
                if isinstance(predicatable, SQLTableInstance):
                    modified_predicatable = SQLTableInstance.build(self._schema, predicatable.table(), copied_predicates, alias=modified_query.alias(predicatable))
                    modified_query.replace_node(predicatable, modified_predicatable)
                else:
                    raise NotImplementedError()
                if monotonicity == 1:
                    left_queries = [query]
                    right_queries = [modified_query]
                else:
                    left_queries = [modified_query]
                    right_queries = [query]
                return CardinalityRelation(left_queries, right_queries, RelationType.GREATEREQUAL)
        return None
