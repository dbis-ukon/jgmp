
from typing import List
from query.graphlike_query import GraphlikeQuery


def detect_cross_product(query: GraphlikeQuery) -> bool:
    # TODO: cyclic query graphs
    return len(query.nodes()) - 1 > len(query.edges())


def eliminate_cross_product(query: GraphlikeQuery) -> List[GraphlikeQuery]:
    factor_queries = []
    current_nodes = None
    for node, edges in query.traverse_query():
        if not node.virtual():
            if len(edges) == 0:
                if current_nodes is not None:
                    factor_queries.append(query.build_subquery(current_nodes, -1))
                current_nodes = []
            current_nodes.append(node)
    if current_nodes is not None:
        factor_queries.append(query.build_subquery(current_nodes, -1))
    return factor_queries
