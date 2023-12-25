

from typing import Dict, FrozenSet
from query.graphlike_query import GraphlikeQuery
from query.query_node import QueryNode
from schema.graphlike_schema import GraphlikeSchema
from cardinality_estimator.cardinality_estimator import CardinalityEstimator


class PropheticAliasCardinalityEstimator(CardinalityEstimator):
    def __init__(self,
                 schema: GraphlikeSchema,
                 group_cardinalities: Dict[FrozenSet[str], float],
                 throw_key_error: bool = True) -> None:
        super().__init__(False)
        self._schema = schema
        self._group_cardinalities = group_cardinalities
        self._throw_key_error = throw_key_error

    def _estimate(self, query: GraphlikeQuery) -> float:
        node_set = frozenset([(query.alias(node), node.hash()) for node in query.nodes() if not node.virtual()])
        if node_set in self._group_cardinalities:
            return self._group_cardinalities[node_set]
        elif self._throw_key_error:
            print(query.text())
            assert(node_set in self._group_cardinalities)
        else:
            return -1
