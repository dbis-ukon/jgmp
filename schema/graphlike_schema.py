
from schema.schema_edge import SchemaEdge
from typing import Dict, List, Tuple
from schema.schema_node import SchemaNode


class GraphlikeSchema:
    def __init__(self, nodes: List[SchemaNode], edges: List[Tuple[SchemaNode, SchemaEdge, SchemaNode]]) -> None:
        self._nodes = nodes
        self._node_dict = {node.name(): node for node in nodes}

        self._edges_from: Dict[SchemaNode, List[Tuple[SchemaEdge, SchemaNode]]] = {node: [] for node in nodes}
        self._edges_to: Dict[SchemaNode, List[Tuple[SchemaEdge, SchemaNode]]] = {node: [] for node in nodes}

        self._edge_dict = {edge.name(): edge for _, edge, _ in edges}

        for from_node, edge, to_node in edges:
            assert(from_node in nodes)
            assert(to_node in nodes)
            self._edges_from[from_node].append((edge, to_node))
            self._edges_to[to_node].append((edge, from_node))

    def nodes(self) -> List[SchemaNode]:
        return self._nodes

    def node(self, name: str) -> SchemaNode:
        assert(name in self._node_dict)
        return self._node_dict[name]

    def edges(self) -> List[SchemaEdge]:
        edges = []
        for name in self._edge_dict:
            edges.append(self._edge_dict[name])
        return edges

    def edge(self, name: str) -> SchemaEdge:
        assert(name in self._edge_dict)
        return self._edge_dict[name]

    def edges_from(self, node: SchemaNode) -> List[Tuple[SchemaEdge, SchemaNode]]:
        return self._edges_from[node]

    def edges_to(self, node: SchemaNode) -> List[Tuple[SchemaEdge, SchemaNode]]:
        return self._edges_to[node]
