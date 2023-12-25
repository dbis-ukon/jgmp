

from typing import Dict, FrozenSet
from query.graphlike_query import GraphlikeQuery
from query.query_node import QueryNode
from query.sql.sql_query import SQLQuery
from schema.graphlike_schema import GraphlikeSchema
from cardinality_estimator.cardinality_estimator import CardinalityEstimator


class PropheticCardinalityEstimator(CardinalityEstimator):
    def __init__(self,
                 schema: GraphlikeSchema,
                 group_cardinalities: Dict[SQLQuery, Dict[FrozenSet[QueryNode], float]],
                 throw_key_error: bool = True) -> None:
        super().__init__(False)
        self._schema = schema
        self._group_cardinalities = {node_set: group_cardinalities[query][node_set] for query in group_cardinalities for node_set in group_cardinalities[query]}
        self._throw_key_error = throw_key_error

    def _estimate(self, query: GraphlikeQuery) -> float:
        node_set = frozenset([node for node in query.nodes() if not node.virtual()])
        if node_set in self._group_cardinalities:
            return self._group_cardinalities[node_set]
        elif self._throw_key_error:
            print(query.text())
            assert(node_set in self._group_cardinalities)
        else:
            return -1
