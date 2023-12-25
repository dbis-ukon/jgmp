
from __future__ import annotations
from abc import abstractmethod
from query.predicatable import Predicatable
from query.query_edge import QueryEdge
from query.query_node import QueryNode
from typing import Dict, FrozenSet, Iterator, List, Optional, Tuple


class GraphlikeQuery:
    def __init__(self,
                 query_language: str,
                 nodes: List[QueryNode],
                 edges: List[Tuple[QueryNode, QueryEdge, QueryNode]],
                 cardinality_estimates: Optional[Dict[str, float]] = None,
                 name: Optional[str] = None,
                 query_id: Optional[int] = None) -> None:
        self._query_language = query_language

        self._nodes = nodes
        self._edges_from: Dict[QueryNode, List[Tuple[QueryEdge, QueryNode]]] = {node: [] for node in nodes}
        self._edges_to: Dict[QueryNode, List[Tuple[QueryEdge, QueryNode]]] = {node: [] for node in nodes}

        self._alias: Dict[Predicatable, str] = {}
        self._next_node_alias = 1
        self._next_edge_alias = 1

        for node in nodes:
            if isinstance(node, Predicatable):
                self._alias[node] = "n%d" % self._next_node_alias
                self._next_node_alias += 1

        for _, edge, _ in edges:
            if isinstance(edge, Predicatable):
                self._alias[edge] = "e%d" % self._next_edge_alias
                self._next_edge_alias += 1

        for from_node, edge, to_node in edges:
            assert(from_node in nodes)
            assert(to_node in nodes)
            self._edges_from[from_node].append((edge, to_node))
            self._edges_to[to_node].append((edge, from_node))

        if cardinality_estimates is None:
            self._cardinality_estimates = {}
        else:
            self._cardinality_estimates = cardinality_estimates

        self._name = name
        self._id = query_id

    def name(self) -> Optional[str]:
        return self._name

    def id(self) -> Optional[int]:
        return self._id

    @abstractmethod
    def text(self) -> str:
        pass

    def cardinality_estimates(self) -> Dict[str, float]:
        return self._cardinality_estimates

    def alias(self, predicatable: Predicatable) -> str:
        assert(predicatable in self._alias)
        return self._alias[predicatable]

    def aliases(self) -> Dict[Predicatable, str]:
        return self._alias

    def predicatable(self, alias: str) -> Optional[Predicatable]:
        for predicatable in self._alias:
            if self._alias[predicatable] == alias:
                return predicatable
        return None

    def nodes(self) -> List[QueryNode]:
        return self._nodes

    def edges_from(self, node: QueryNode) -> List[Tuple[QueryEdge, QueryNode]]:
        return self._edges_from[node]

    def edges_to(self, node: QueryNode) -> List[Tuple[QueryEdge, QueryNode]]:
        return self._edges_to[node]

    def edges_incident(self, node: QueryNode) -> List[Tuple[QueryNode, QueryEdge, QueryNode]]:
        edges_from = [(node, edge, to_node) for edge, to_node in self.edges_from(node)]
        edges_to = [(from_node, edge, node) for edge, from_node in self.edges_to(node)]
        return edges_from + edges_to

    def edges_incident_direction(self, node: QueryNode) -> List[Tuple[QueryNode, QueryEdge, QueryNode, bool]]:
        edges_from = [(node, edge, to_node, True) for edge, to_node in self.edges_from(node)]
        edges_to = [(from_node, edge, node, False) for edge, from_node in self.edges_to(node)]
        return edges_from + edges_to

    def edges(self) -> List[Tuple[QueryNode, QueryEdge, QueryNode]]:
        edges = []
        for start_node in self._nodes:
            for edge, end_node in self.edges_from(start_node):
                edges.append((start_node, edge, end_node))

        return edges

    def node_order(self) -> List[QueryNode]:
        return [node for node, _ in self.traverse_query()]

    def traverse_query(self) -> Iterator[Tuple[QueryNode, List[Tuple[QueryNode, QueryEdge, QueryNode]]]]:
        visited = set()
        while len(visited) < len(self._nodes):
            start = None
            for node in self._nodes:
                if node not in visited:
                    start = node
                    break
            visited.add(start)
            yield start, []
            neighboring = [node for _, node in self.edges_from(start) + self.edges_to(start)]
            while len(neighboring) > 0:
                next_node = neighboring.pop()
                if next_node not in visited:
                    incident_visited = []

                    for from_node, edge, to_node in self.edges_incident(next_node):
                        if from_node in visited or to_node in visited:
                            incident_visited.append((from_node, edge, to_node))

                    visited.add(next_node)
                    neighboring += [node for _, node in self.edges_from(next_node) + self.edges_to(next_node)]
                    yield next_node, incident_visited

    def predicate_string(self) -> str:
        predicate_strings = []

        for i, node in enumerate(self._nodes):
            if isinstance(node, Predicatable) and len(node.predicates()) > 0:
                predicate_strings.append(node.predicate_string(self._alias[node]))

        for i, edge_triple in enumerate(self.edges()):
            _, edge, _ = edge_triple
            if isinstance(edge, Predicatable) and len(edge.predicates()) > 0:
                predicate_strings.append(edge.predicate_string(self._alias[edge]))

        if len(predicate_strings) == 0:
            return ""
        else:
            return "\nWHERE " + "\n\tAND ".join(predicate_strings)

    def replace_node(self, old: QueryNode, new: QueryNode):
        self._nodes.append(new)
        self._nodes.remove(old)
        if old in self._alias:
            self._alias[new] = self._alias[old]
            del self._alias[old]
        self._edges_from[new] = self._edges_from[old]
        del self._edges_from[old]
        self._edges_to[new] = self._edges_to[old]
        del self._edges_to[old]
        for _, to_node in self._edges_from[new]:
            new_edges_to = []
            for edge, from_node in self._edges_to[to_node]:
                if from_node == old:
                    new_edges_to.append((edge, new))
                else:
                    new_edges_to.append((edge, from_node))
            self._edges_to[to_node] = new_edges_to
        for _, from_node in self._edges_to[new]:
            new_edges_from = []
            for edge, to_node in self._edges_from[from_node]:
                if to_node == old:
                    new_edges_from.append((edge, new))
                else:
                    new_edges_from.append((edge, to_node))
            self._edges_from[from_node] = new_edges_from

    def add_node(self, node: QueryNode):
        self._nodes.append(node)
        self._edges_from[node] = []
        self._edges_to[node] = []
        if isinstance(node, Predicatable):
            self._alias[node] = "n%d" % self._next_node_alias
            self._next_node_alias += 1

    def add_edge(self, from_node: QueryNode, edge: QueryEdge, to_node: QueryNode):
        self._edges_from[from_node].append((edge, to_node))
        self._edges_to[to_node].append((edge, from_node))
        if isinstance(edge, Predicatable):
            self._alias[edge] = "e%d" % self._next_node_alias
            self._next_edge_alias += 1

    @abstractmethod
    def build_subquery(self, nodes: FrozenSet[QueryNode], cardinality: int) -> GraphlikeQuery:
        pass

    @abstractmethod
    def shallow_copy(self) -> GraphlikeQuery:
        pass

    @abstractmethod
    def deep_copy(self) -> Tuple[GraphlikeQuery, Dict[Predicatable, Predicatable]]:
        pass
