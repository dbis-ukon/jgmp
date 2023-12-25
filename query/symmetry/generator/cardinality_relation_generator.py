

from abc import abstractmethod
from typing import Optional
from query.graphlike_query import GraphlikeQuery
from query.symmetry.cardinality_relation import CardinalityRelation
from schema.graphlike_schema import GraphlikeSchema


class CardinalityRelationGenerator:
    def __init__(self, schema: GraphlikeSchema) -> None:
        self._schema = schema

    @abstractmethod
    def generate(self, query: GraphlikeQuery) -> Optional[CardinalityRelation]:
        pass
