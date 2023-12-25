
from query.predicate import Predicate
from query.predicate_generators import PREDICATE_GENERATORS
from query.query_edge import QueryEdge
from query.query_generator import QueryGenerator
from query.query_node import QueryNode
from typing import List, Tuple
from schema.value_picker import ValuePicker
from schema.schema_edge import SchemaEdge
from schema.attribute import Attribute
from schema.attributable import Attributable
from schema.schema_node import SchemaNode
from query.graphlike_query import GraphlikeQuery
from schema.graphlike_schema import GraphlikeSchema
from random import Random
import abc


def geometric_distribution(random: Random, p: float) -> int:
    i = 0
    while random.random() > p:
        i += 1
    return i


class QueryGeneratorSchema(QueryGenerator):
    def __init__(self,
                 schema: GraphlikeSchema,
                 value_picker: ValuePicker,
                 default_min_nodes: int,
                 default_max_nodes: int,
                 default_predicate_factor: float,
                 seed: int = 42
                 ) -> None:
        self._random = Random(seed)
        self._schema = schema
        self._value_picker = value_picker
        self._default_min_nodes = default_min_nodes
        self._default_max_nodes = default_max_nodes
        self._default_predicate_factor = default_predicate_factor

    def generate_query_default(self) -> GraphlikeQuery:
        return self.generate_query(self._default_min_nodes, self._default_max_nodes, self._default_predicate_factor)

    @abc.abstractmethod
    def generate_query(self,
                       min_nodes: int,
                       max_nodes: int,
                       predicate_factor: float) -> GraphlikeQuery:
        pass

    def _generate_nodes_and_edges(self,
                                  min_nodes: int,
                                  max_nodes: int,
                                  predicate_factor: float,
                                  unique_out_edges: bool) -> Tuple[List[QueryNode], List[Tuple[QueryNode, QueryEdge, QueryNode]]]:
        number_of_nodes = self._random.choice(range(min_nodes, max_nodes + 1))
        number_of_predicates = 1 + geometric_distribution(self._random, predicate_factor)

        start_node = self._random.choice(self._schema.nodes())
        schema_nodes = [start_node]
        schema_edges = []
        possible_edges = self._possible_edges(start_node, 0)
        if start_node.is_leaf():
            possible_edges = [self._random.choice(possible_edges)]
        predicate_candidates: List[Tuple[bool, int]] = []
        if isinstance(start_node, Attributable):
            predicate_candidates += [(True, 0)] * len(self._possible_predicates(start_node))

        for i in range(1, number_of_nodes):
            if(len(possible_edges) == 0):
                break
            edge_number = self._random.choice(range(len(possible_edges)))
            node_number, edge, other_node, natural_direction = possible_edges.pop(edge_number)
            schema_nodes.append(other_node)
            if isinstance(other_node, Attributable):
                predicate_candidates += [(True, i)] * len(self._possible_predicates(other_node))
            if isinstance(edge, Attributable):
                predicate_candidates += [(False, i - 1)] * len(self._possible_predicates(edge))
            if not other_node.is_leaf():
                additional_edges = self._possible_edges(other_node, i)
                if unique_out_edges:
                    additional_edges = [additional_edge for additional_edge in additional_edges if additional_edge[1] != edge]
                possible_edges += additional_edges

            if natural_direction:
                schema_edges.append((node_number, edge, i))
            else:
                schema_edges.append((i, edge, node_number))

        if number_of_predicates > len(predicate_candidates):
            number_of_predicates = len(predicate_candidates)
        predicates = self._random.sample(predicate_candidates, number_of_predicates)

        nodes = []
        edges = []

        for i, schema_node in enumerate(schema_nodes):
            node_predicates = sum([1 for is_node, j in predicates if is_node and i == j])
            nodes.append(self._generate_node(schema_node, node_predicates))

        for i, edge_triple in enumerate(schema_edges):
            from_number, schema_edge, to_number = edge_triple
            edge_predicates = sum([1 for is_node, j in predicate_candidates if (not is_node) and i == j])
            edge = self._generate_edge(schema_edge, edge_predicates)
            edges.append((nodes[from_number], edge, nodes[to_number]))

        return nodes, edges

    def _possible_edges(self, node: SchemaNode, node_number: int) -> List[Tuple[int, SchemaEdge, SchemaNode, bool]]:
        natural_edges = [(node_number, edge, to_node, True) for edge, to_node in self._schema.edges_from(node)]
        reverse_edges = [(node_number, edge, from_node, False) for edge, from_node in self._schema.edges_to(node)]
        return natural_edges + reverse_edges

    @abc.abstractmethod
    def _generate_node(self, schema_node: SchemaNode, num_predicates: int) -> Tuple[QueryNode, int]:
        pass

    @abc.abstractmethod
    def _generate_edge(self, schema_edge: SchemaEdge, num_predicates: int) -> Tuple[QueryEdge, int]:
        pass

    @abc.abstractmethod
    def _possible_predicates(self, entity: Attributable) -> List[Attribute]:
        pass

    def _generate_predicates(self, entity: Attributable, num_predicates: int) -> List[Predicate]:
        attributes = self._random.sample(self._possible_predicates(entity), num_predicates)
        predicates = []
        for attribute in attributes:
            predicate_generators = PREDICATE_GENERATORS[attribute.data_type().encoding_type()]
            generators = [generator for generator in predicate_generators]
            weights = [predicate_generators[generator] for generator in predicate_generators]
            generator = self._random.choices(generators, weights)[0]
            predicates += generator(self._value_picker, self._random, entity, attribute)
        return predicates

    @abc.abstractmethod
    def _get_random_value(self, entity: Attributable, attribute: Attribute):
        pass
