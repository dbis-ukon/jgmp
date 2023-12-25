

from typing import Optional
from query.graphlike_query import GraphlikeQuery
from query.symmetry.cardinality_relation import CardinalityRelation
from query.symmetry.generator.cardinality_relation_generator import CardinalityRelationGenerator


class CardRelMergeMonotonicity(CardinalityRelationGenerator):
    def generate(self, query: GraphlikeQuery) -> Optional[CardinalityRelation]:
        return None
