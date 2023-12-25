
import random
from typing import Optional
from query.graphlike_query import GraphlikeQuery
from query.predicate import Predicate
from query.predicate_generators import like_value
from query.sql.sql_table_instance import SQLTableInstance
from query.symmetry.cardinality_relation import CardinalityRelation, RelationType
from query.symmetry.generator.cardinality_relation_generator import CardinalityRelationGenerator
from schema.comparison_operator import OPERATORS
from schema.graphlike_schema import GraphlikeSchema
from schema.value_picker import ValuePicker


class CardRelOrMonotonicity(CardinalityRelationGenerator):
    def __init__(self, schema: GraphlikeSchema, value_picker: ValuePicker) -> None:
        super().__init__(schema)
        self._value_picker = value_picker

    def generate(self, query: GraphlikeQuery) -> Optional[CardinalityRelation]:
        or_query = query.shallow_copy()
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
                    copied_disjunction.append(predicate)
                    if not modified and isinstance(predicate, Predicate) and ((predicate.operator() == OPERATORS["="] and predicate.positive()) or predicate.operator() == OPERATORS["LIKE"]):
                        labels = predicatable.labels()
                        assert(len(labels) == 1)  # TODO: multiple labels
                        node = labels[0]
                        attribute = predicate.attribute()
                        if predicate.operator() == OPERATORS["="]:
                            value = self._value_picker.pick_random(node, attribute)
                        else:
                            value = like_value(self._value_picker, random.Random(), node, attribute, not predicate.value().startswith("%"))
                        copied_disjunction.append(Predicate(attribute, predicate.operator(), value, positive=predicate.positive()))
                        modified = True
                copied_predicates.append(copied_disjunction)
            if modified:
                if isinstance(predicatable, SQLTableInstance):
                    or_predicatable = SQLTableInstance.build(self._schema, predicatable.table(), copied_predicates, alias=or_query.alias(predicatable))
                    or_query.replace_node(predicatable, or_predicatable)
                else:
                    raise NotImplementedError()
                return CardinalityRelation([or_query], [query], RelationType.GREATEREQUAL)
        return None
