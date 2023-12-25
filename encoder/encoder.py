import hashlib
import re
from decimal import Decimal

from torch import LongTensor, FloatTensor, BoolTensor
from torch_geometric.data import HeteroData

from encoder.sampler import Sampler
from query.sql.sql_query import SQLQuery
from query.symmetry.cardinality_relation import CardinalityRelation, RelationType
from query.symmetry.eliminate_lesser import eliminate_lesser_query
from query_data.bulk_cardinality_query_data import BulkCardinalityQueryData
from query_data.bulk_light_cardinality_query_data import BulkLightCardinalityQueryData
from query_data.bulk_light_relation_query_data import BulkLightRelationQueryData
from query_data.bulk_mscn_cardinality_query_data import BulkMSCNCardinalityQueryData
from query_data.bulk_mscn_relation_query_data import BulkMSCNRelationQueryData
from query_data.bulk_relation_query_data import BulkRelationQueryData
from query_data.cardinality_query_data import CardinalityQueryData
from query_data.graphlike_query_data import GraphlikeQueryData
from schema.attribute import Attribute, NumericAttribute
from schema.comparison_operator import OPERATORS
from schema.data_type import EncodingType
from query.query_node import QueryNode
from query.query_edge import EdgeDirection, QueryEdge
from query.predicate import Predicate
from query.predicatable import Predicatable
from schema.schema_edge import SchemaEdge
from schema.attributable import Attributable
from query.graphlike_query import GraphlikeQuery
from schema.schema_node import SchemaNode
from typing import Dict, List, Optional, Tuple, TypeVar, Callable, Any
from schema.graphlike_schema import GraphlikeSchema
from plan.sql.properties.column_property import ColumnProperty
import numpy as np
import torch
from enum import Enum
from schema.sql.sql_foreign_key import SQLForeignKey
from schema.sql.sql_schema import SQLSchema


class CostMode(Enum):
    FIRST = 1
    OPTIMAL = 2


class Encoder:
    def __init__(self,
                 schema: GraphlikeSchema,
                 sampler: Optional[Sampler] = None,
                 eliminate_lesser: bool = True,
                 num_buckets: int = 10,
                 attribute_table_order: Optional[List[str]] = None
                 ) -> None:
        self._schema = schema
        self._sampler = sampler
        self._eliminate_lesser = eliminate_lesser
        self._num_buckets = num_buckets

        node_labels = []
        edge_labels = []
        attributes = []
        attribute_nodes = {}
        operators = []

        for node in schema.nodes():
            node_labels.append(node)
            if isinstance(node, Attributable):
                for attribute in node.attributes():
                    attributes.append(attribute)
                    if attribute not in attribute_nodes:
                        attribute_nodes[attribute] = []
                    attribute_nodes[attribute].append(node)
                    operators += attribute.data_type().operators()

            for edge, _ in schema.edges_from(node):
                edge_labels.append(edge)
                if isinstance(edge, Attributable):
                    for attribute in edge.attributes():
                        attributes.append(attribute)
                        operators += attribute.data_type().operators()

        self.num_nodes, self.node_dict, self.node_index = Encoder.one_hot_encoding(node_labels, sort_key=lambda n: n.name())
        self.no_node_labels = np.zeros(self.num_nodes)
        self.all_node_labels = np.ones(self.num_nodes)
        self.num_edges, self.edge_dict, _ = Encoder.one_hot_encoding(edge_labels, sort_key=lambda n: n.name())
        self.no_edge_labels = np.zeros(self.num_edges)
        self.all_edge_labels = np.ones(self.num_edges)
        if isinstance(schema, SQLSchema):
            for attribute in attribute_nodes:
                assert(len(attribute_nodes[attribute]) == 1)
            if attribute_table_order is None:
                self.num_attributes, self.attribute_dict, _ = Encoder.one_hot_encoding(attributes, sort_key=lambda a: (a.name(), attribute_nodes[a][0].name()))
            else:
                table_order_dict = {table: i for i, table in enumerate(attribute_table_order)}
                self.num_attributes, self.attribute_dict, _ = Encoder.one_hot_encoding(attributes, sort_key=lambda a: (table_order_dict[attribute_nodes[a][0].name()], a.name()))
        else:
            raise NotImplementedError()
        self.num_operators, self.operator_dict, _ = Encoder.one_hot_encoding(operators, sort_key=lambda o: o.symbol())

    def sampler(self) -> Optional[Sampler]:
        return self._sampler

    def set_sampler(self, sampler: Sampler):
        self._sampler = sampler

    def attribute_encoding_size(self) -> int:
        return self.num_attributes

    def node_label_encoding_size(self) -> int:
        return self.num_nodes

    def node_encoding_size(self) -> int:
        node_encoding_size = self.node_label_encoding_size() + 1
        if self._sampler is not None:
            node_encoding_size += self._sampler.bitmap_size()
        return node_encoding_size

    def edge_encoding_size(self) -> int:
        return self.num_edges

    def edge_direction_size(self) -> int:
        return 2

    def predicate_encoding_size(self) -> int:
        return self.num_attributes + self.num_operators + 2

    T = TypeVar("T")

    @staticmethod
    def one_hot_encoding(elements: List[T], sort_key: Optional[Callable[[T], Any]] = None) -> Tuple[int, Dict[T, np.ndarray], Dict[T, int]]:
        elements = list(set(elements))
        list.sort(elements, key=sort_key)

        length = len(elements)

        one_hot = {}
        index = {}
        for i, element in enumerate(elements):
            array = np.zeros(length)
            array[i] = 1
            one_hot[element] = array
            index[element] = i

        return length, one_hot, index

    def _encode_base(self, query: GraphlikeQuery) -> Tuple[GraphlikeQueryData, Dict[QueryNode, int], Dict[QueryEdge, int]]:
        if self._eliminate_lesser:
            eliminate_lesser_query(query)
        node_labels = []
        node_predicates = []
        node_disjunction_index = []
        node_conjunction_index = []
        node_id = {}
        disjunction_id = 0
        for i, node in enumerate(query.nodes()):
            node_labels.append(self.encode_node_with_samples(node))
            node_id[node] = i
            if isinstance(node, Predicatable):
                for disjunction in node.predicates():
                    for predicate in disjunction:
                        node_predicates.append(self.encode_predicate(predicate))
                        node_disjunction_index.append(disjunction_id)
                    node_conjunction_index.append(i)
                    disjunction_id += 1

        edge_labels = []
        edge_index = []
        edge_directions = []
        edge_directions_reversed = []
        edge_predicates = []
        edge_disjunction_index = []
        edge_conjunction_index = []
        edge_id = {}
        disjunction_id = 0
        for i, triple in enumerate(query.edges()):
            start_node, edge, end_node = triple
            edge_labels.append(self.encode_edge_labels(edge.labels()))
            edge_index.append([node_id[start_node], node_id[end_node]])
            edge_directions.append(self.encode_edge_direction(edge.direction()))
            edge_directions_reversed.append(self.encode_edge_direction(EdgeDirection.reverse(edge.direction())))
            edge_id[edge] = i
            if isinstance(edge, Predicatable):
                for disjunction in edge.predicates():
                    for predicate in disjunction:
                        edge_predicates.append(self.encode_predicate(predicate))
                        edge_disjunction_index.append(disjunction_id)
                    edge_conjunction_index.append(i)
                    disjunction_id += 1

        node_labels = torch.Tensor(np.stack(node_labels))
        if node_predicates == []:
            node_predicates = torch.empty((0, self.predicate_encoding_size()))
            node_disjunction_index = torch.empty((0), dtype=torch.int64)
            node_conjunction_index = torch.empty((0), dtype=torch.int64)
        else:
            node_predicates = torch.Tensor(np.stack(node_predicates))
            node_disjunction_index = torch.LongTensor(np.stack(node_disjunction_index))
            node_conjunction_index = torch.LongTensor(np.stack(node_conjunction_index))

        if edge_labels == []:
            edge_labels = torch.empty((0, self.edge_encoding_size()))
            edge_index = torch.empty((2, 0), dtype=torch.int64)
            edge_directions = torch.empty((0, self.edge_direction_size()))
            edge_directions_reversed = torch.empty((0, self.edge_direction_size()))
        else:
            edge_labels = torch.Tensor(np.stack(edge_labels))
            edge_index = torch.LongTensor(np.transpose(np.stack(edge_index)))
            edge_directions = torch.Tensor(np.stack(edge_directions))
            edge_directions_reversed = torch.Tensor(np.stack(edge_directions_reversed))

        if edge_predicates == []:
            edge_predicates = torch.empty((0, self.predicate_encoding_size()))
            edge_disjunction_index = torch.empty((0), dtype=torch.int64)
            edge_conjunction_index = torch.empty((0), dtype=torch.int64)
        else:
            edge_predicates = torch.Tensor(np.stack(edge_predicates))
            edge_disjunction_index = torch.LongTensor(np.stack(edge_disjunction_index))
            edge_conjunction_index = torch.LongTensor(np.stack(edge_conjunction_index))

        node_count = torch.Tensor([[len(query.nodes())]])

        return (GraphlikeQueryData(node_labels,
                                   edge_index,
                                   edge_labels,
                                   edge_directions,
                                   edge_directions_reversed,
                                   node_predicates,
                                   node_disjunction_index,
                                   node_conjunction_index,
                                   edge_predicates,
                                   edge_disjunction_index,
                                   edge_conjunction_index,
                                   node_count),
                node_id,
                edge_id)

    def encode_graphlike_query(self, query: GraphlikeQuery) -> GraphlikeQueryData:
        return self._encode_base(query)[0]

    def encode_node_with_samples(self, node: QueryNode) -> np.ndarray:
        node_labels = self.encode_node_labels(node.labels())
        cardinality_scaled = np.log(node.cardinality())
        if self._sampler is not None:
            sample_bitmap = self._sampler.bitmap_padded(node)
            return np.concatenate([node_labels, np.array([cardinality_scaled]), sample_bitmap])
        return np.concatenate([node_labels, np.array([cardinality_scaled])])

    def encode_node_labels(self, labels: List[SchemaNode]) -> np.ndarray:
        if len(labels) == 0:
            return self.no_node_labels
        elif len(labels) == 1:
            return self.node_dict[labels[0]]
        elif len(labels) == self.num_nodes:
            return self.all_node_labels
        else:
            array = np.zeros(self.num_nodes)
            for label in labels:
                array += self.node_dict[label]
            return array

    def encode_edge_direction(self, direction: EdgeDirection) -> np.ndarray:
        if direction == EdgeDirection.NATURAL:
            return np.array([1, 0])
        elif direction == EdgeDirection.REVERSED:
            return np.array([0, 1])
        elif direction == EdgeDirection.UNDIRECTED:
            return np.array([1, 1])

    def encode_edge_labels(self, labels: List[SchemaEdge]) -> np.ndarray:
        if len(labels) == 0:
            return self.no_edge_labels
        elif len(labels) == 1:
            return self.edge_dict[labels[0]]
        elif len(labels) == self.num_edges:
            return self.all_edge_labels
        else:
            array = np.zeros(self.num_edges)
            for label in labels:
                array += self.edge_dict[label]
            return array

    def encode_attribute(self, attribute: Attribute) -> np.ndarray:
        return self.attribute_dict[attribute]

    def encode_predicate(self, predicate: Predicate) -> np.ndarray:
        attribute = predicate.attribute()
        operator = predicate.operator()
        value = predicate.value()
        positive = predicate.positive()

        attribute_array = self.encode_attribute(attribute)
        operator_array = self.operator_dict[operator]
        if attribute.data_type().encoding_type() == EncodingType.NUMERIC and value is not None:
            value_normalized = np.array([(value - attribute.minimum()) / (attribute.maximum() - attribute.minimum())])
        else:
            value_normalized = np.array([-1])  # TODO: string predicate encodings

        if positive:
            positive_array = np.array([1])
        else:
            positive_array = np.array([0])

        return np.concatenate([positive_array, attribute_array, operator_array, value_normalized])

    def encode_cardinality(self, query: GraphlikeQuery, cardinality: int) -> CardinalityQueryData:
        cardinality = torch.Tensor([max(cardinality, 1)])
        data = self.encode_graphlike_query(query)

        return CardinalityQueryData(data.x,
                                    data.edge_index,
                                    data.edge_attr,
                                    data.edge_directions,
                                    data.edge_directions_reversed,
                                    data.node_predicates,
                                    data.node_disjunction_index,
                                    data.node_conjunction_index,
                                    data.edge_predicates,
                                    data.edge_disjunction_index,
                                    data.edge_conjunction_index,
                                    data.node_count,
                                    cardinality)

    def _add_self_loops(self, hetero_data: HeteroData):
        for table in self._schema.nodes():
            loop_index = list(range(hetero_data[table.name()].x.size()[0]))
            hetero_data[table.name(), "self_loop", table.name()].edge_index = torch.LongTensor([loop_index, loop_index])

    def _bulk_encode_base(self,
                          queries: List[SQLQuery]
                          ) -> Tuple[LongTensor,  # node_index
                                     LongTensor,  # edge_index
                                     FloatTensor,  # edge_labels
                                     LongTensor,  # shared_node_labels
                                     FloatTensor,  # shared_node_cardinalities
                                     FloatTensor,  # shared_node_samples
                                     FloatTensor,  # shared_node_predicates
                                     LongTensor,  # shared_node_disjunction_index
                                     LongTensor,  # shared_node_conjunction_index
                                     LongTensor  # my_batch
                                     ]:
        shared_nodes = {}
        virtual_tables = {}
        node_index = []
        edge_index = []
        edge_labels = []
        shared_node_labels = []
        shared_node_cardinalities = []
        shared_node_samples = []
        shared_node_predicates = []
        shared_node_disjunction_index = []
        shared_node_conjunction_index = []
        my_batch = []

        node_count = 0
        shared_node_count = 0
        shared_disjunction_count = 0
        for query_count, query in enumerate(queries):
            nodes = query.nodes()
            node_dict = {}
            for node in nodes:
                virtual = node.virtual()
                if node not in shared_nodes and (not virtual or node.table() not in virtual_tables):
                    if virtual:
                        virtual_tables[node.table()] = shared_node_count
                    else:
                        shared_nodes[node] = shared_node_count
                    shared_node_labels.append(self.node_index[node.table()])
                    shared_node_cardinalities.append(np.log(node.cardinality()))
                    if self._sampler is not None:
                        shared_node_samples.append(self._sampler.bitmap_padded(node, query.alias(node)))
                    for disjunction in node.predicates():
                        for predicate in disjunction:
                            shared_node_predicates.append(self.encode_predicate(predicate))
                            shared_node_disjunction_index.append(shared_disjunction_count)
                        shared_node_conjunction_index.append(shared_node_count)
                        shared_disjunction_count += 1
                    shared_node_count += 1
                if virtual:
                    node_id = virtual_tables[node.table()]
                else:
                    node_id = shared_nodes[node]
                node_index.append(node_id)
                my_batch.append(query_count)
                node_dict[node] = node_count
                node_count += 1

            for from_node in nodes:
                for edge, to_node in query.edges_from(from_node):
                    edge_labels.append(self.encode_edge_labels(edge.labels()))
                    edge_index.append((node_dict[from_node], node_dict[to_node]))


        node_index = torch.LongTensor(node_index)
        if edge_index == []:
            edge_index = torch.empty((2, 0), dtype=torch.int64)
            edge_labels = torch.empty((0, self.edge_encoding_size()))
        else:
            edge_index = torch.LongTensor(np.transpose(np.stack(edge_index)))
            edge_labels = torch.Tensor(np.stack(edge_labels))
        shared_node_labels = torch.LongTensor(np.stack(shared_node_labels))
        shared_node_cardinalities = torch.Tensor(shared_node_cardinalities)
        shared_node_samples = torch.Tensor(np.stack(shared_node_samples))
        if shared_node_predicates == []:
            shared_node_predicates = torch.empty((0, self.predicate_encoding_size()))
            shared_node_disjunction_index = torch.empty((0), dtype=torch.int64)
            shared_node_conjunction_index = torch.empty((0), dtype=torch.int64)
        else:
            shared_node_predicates = torch.Tensor(np.stack(shared_node_predicates))
            shared_node_disjunction_index = torch.LongTensor(np.stack(shared_node_disjunction_index))
            shared_node_conjunction_index = torch.LongTensor(np.stack(shared_node_conjunction_index))
        my_batch = torch.LongTensor(my_batch)

        return (node_index,
                edge_index,
                edge_labels,
                shared_node_labels,
                shared_node_cardinalities,
                shared_node_samples,
                shared_node_predicates,
                shared_node_disjunction_index,
                shared_node_conjunction_index,
                my_batch)

    def bulk_encode_cardinality(self, queries: List[SQLQuery]) -> BulkCardinalityQueryData:
        base_encodings = self._bulk_encode_base(queries)
        cardinalities = []
        for query in queries:
            cardinality_estimates = query.cardinality_estimates()
            if "true" in cardinality_estimates:
                cardinalities.append(max(cardinality_estimates["true"], 1))
            else:
                cardinalities.append(-1)
        cardinalities = torch.Tensor(cardinalities)
        return BulkCardinalityQueryData(*base_encodings, cardinalities)

    def bulk_encode_relation(self, cardinality_relation: CardinalityRelation) -> BulkRelationQueryData:
        queries = []
        left = [[], []]
        right = [[], []]
        for query in cardinality_relation.left():
            left[0].append(len(queries))
            left[1].append(0)
            queries.append(query)
        for query in cardinality_relation.right():
            right[0].append(len(queries))
            right[1].append(0)
            queries.append(query)
        equal = [cardinality_relation.type() == RelationType.EQUAL]

        base_encodings = self._bulk_encode_base(queries)
        left = LongTensor(np.stack(left))
        right = LongTensor(np.stack(right))
        equal = BoolTensor(equal)

        return BulkRelationQueryData(*base_encodings, left, right, equal)

    def _bulk_light_encode_base(self,
                          queries: List[SQLQuery]
                          ) -> Tuple[LongTensor,  # node_index
                                     LongTensor,  # edge_index
                                     FloatTensor,  # edge_labels
                                     LongTensor,  # shared_node_labels
                                     FloatTensor,  # shared_node_cardinalities
                                     FloatTensor,  # shared_node_samples
                                     LongTensor  # my_batch
                                     ]:
        shared_nodes = {}
        virtual_tables = {}
        node_index = []
        edge_index = []
        edge_labels = []
        shared_node_labels = []
        shared_node_cardinalities = []
        shared_node_samples = []
        my_batch = []

        node_count = 0
        shared_node_count = 0
        for query_count, query in enumerate(queries):
            nodes = query.nodes()
            node_dict = {}
            for node in nodes:
                virtual = node.virtual()
                if node not in shared_nodes and (not virtual or node.table() not in virtual_tables):
                    if virtual:
                        virtual_tables[node.table()] = shared_node_count
                    else:
                        shared_nodes[node] = shared_node_count
                    shared_node_labels.append(self.node_index[node.table()])
                    shared_node_cardinalities.append(np.log(node.cardinality()))
                    if self._sampler is not None:
                        shared_node_samples.append(self._sampler.bitmap_padded(node, query.alias(node)))
                    shared_node_count += 1
                if virtual:
                    node_id = virtual_tables[node.table()]
                else:
                    node_id = shared_nodes[node]
                node_index.append(node_id)
                my_batch.append(query_count)
                node_dict[node] = node_count
                node_count += 1

            for from_node in nodes:
                for edge, to_node in query.edges_from(from_node):
                    edge_labels.append(self.encode_edge_labels(edge.labels()))
                    edge_index.append((node_dict[from_node], node_dict[to_node]))


        node_index = torch.LongTensor(node_index)
        if edge_index == []:
            edge_index = torch.empty((2, 0), dtype=torch.int64)
            edge_labels = torch.empty((0, self.edge_encoding_size()))
        else:
            edge_index = torch.LongTensor(np.transpose(np.stack(edge_index)))
            edge_labels = torch.Tensor(np.stack(edge_labels))
        shared_node_labels = torch.LongTensor(np.stack(shared_node_labels))
        shared_node_cardinalities = torch.Tensor(shared_node_cardinalities)
        shared_node_samples = torch.Tensor(np.stack(shared_node_samples))
        my_batch = torch.LongTensor(my_batch)

        return (node_index,
                edge_index,
                edge_labels,
                shared_node_labels,
                shared_node_cardinalities,
                shared_node_samples,
                my_batch)

    def bulk_light_encode_cardinality(self, queries: List[SQLQuery]) -> BulkLightCardinalityQueryData:
        base_encodings = self._bulk_light_encode_base(queries)
        cardinalities = []
        for query in queries:
            cardinality_estimates = query.cardinality_estimates()
            if "true" in cardinality_estimates:
                cardinalities.append(max(cardinality_estimates["true"], 1))
            else:
                cardinalities.append(-1)
        cardinalities = torch.Tensor(cardinalities)
        return BulkLightCardinalityQueryData(*base_encodings, cardinalities)

    def bulk_light_encode_relation(self, cardinality_relation: CardinalityRelation) -> BulkLightRelationQueryData:
        queries = []
        left = [[], []]
        right = [[], []]
        for query in cardinality_relation.left():
            left[0].append(len(queries))
            left[1].append(0)
            queries.append(query)
        for query in cardinality_relation.right():
            right[0].append(len(queries))
            right[1].append(0)
            queries.append(query)
        equal = [cardinality_relation.type() == RelationType.EQUAL]

        base_encodings = self._bulk_light_encode_base(queries)
        left = LongTensor(np.stack(left))
        right = LongTensor(np.stack(right))
        equal = BoolTensor(equal)

        return BulkLightRelationQueryData(*base_encodings, left, right, equal)

    def _bulk_mscn_encode_base(self,
                               queries: List[SQLQuery]
                               ) -> Tuple[LongTensor,  # node_index
                                          LongTensor,  # edge_index
                                          FloatTensor,  # edge_labels
                                          LongTensor,  # shared_node_labels
                                          FloatTensor,  # shared_node_label_vectors
                                          FloatTensor,  # shared_node_cardinalities
                                          FloatTensor,  # shared_node_samples
                                          FloatTensor,  # shared_node_predicates
                                          LongTensor,  # shared_node_predicate_index
                                          LongTensor  # my_batch
                                          ]:
        shared_nodes = {}
        virtual_tables = {}
        node_index = []
        edge_index = []
        edge_labels = []

        shared_node_labels = []
        shared_node_label_vectors = []
        shared_node_cardinalities = []
        shared_node_samples = []
        shared_node_predicates = []
        predicate_index_dict = {}
        shared_node_predicate_index = []
        my_batch = []

        node_count = 0
        shared_node_count = 0
        shared_predicate_count = 0
        for query_count, query in enumerate(queries):
            nodes = query.nodes()
            node_dict = {}
            for node in nodes:
                virtual = node.virtual()
                if node not in shared_nodes and (not virtual or node.table() not in virtual_tables):
                    if virtual:
                        virtual_tables[node.table()] = shared_node_count
                    else:
                        shared_nodes[node] = shared_node_count
                    shared_node_labels.append(self.node_index[node.table()])
                    shared_node_label_vectors.append(self.encode_node_labels(node.labels()))
                    shared_node_cardinalities.append(np.log(node.cardinality()))
                    if self._sampler is not None:
                        shared_node_samples.append(self._sampler.bitmap_padded(node))
                    predicates = self._encode_mscn_predicates(node.predicates())
                    shared_node_predicates += predicates
                    predicate_index = []
                    for _ in predicates:
                        predicate_index.append(shared_predicate_count)
                        shared_predicate_count += 1
                    predicate_index_dict[node] = predicate_index
                    shared_node_count += 1
                if virtual:
                    node_id = virtual_tables[node.table()]
                else:
                    node_id = shared_nodes[node]
                if node in predicate_index_dict:
                    for predicate_index in predicate_index_dict[node]:
                        shared_node_predicate_index.append([predicate_index, query_count])
                node_index.append(node_id)
                my_batch.append(query_count)
                node_dict[node] = node_count
                node_count += 1

            for from_node in nodes:
                for edge, to_node in query.edges_from(from_node):
                    edge_labels.append(self.encode_edge_labels(edge.labels()))
                    edge_index.append((node_dict[from_node], node_dict[to_node]))


        node_index = torch.LongTensor(node_index)
        if edge_index == []:
            edge_index = torch.empty((2, 0), dtype=torch.int64)
            edge_labels = torch.empty((0, self.edge_encoding_size()))
        else:
            edge_index = torch.LongTensor(np.transpose(np.stack(edge_index)))
            edge_labels = torch.Tensor(np.stack(edge_labels))
        shared_node_labels = torch.LongTensor(np.stack(shared_node_labels))
        shared_node_label_vectors = torch.Tensor(np.stack(shared_node_label_vectors))
        shared_node_cardinalities = torch.Tensor(shared_node_cardinalities)
        shared_node_samples = torch.Tensor(np.stack(shared_node_samples))
        if shared_node_predicates == []:
            shared_node_predicates = torch.empty((0, self.mscn_predicate_size()))
            shared_node_predicate_index = torch.empty((2, 0), dtype=torch.int64)
        else:
            shared_node_predicates = torch.Tensor(np.stack(shared_node_predicates))
            shared_node_predicate_index = torch.LongTensor(np.transpose(np.stack(shared_node_predicate_index)))
        my_batch = torch.LongTensor(my_batch)

        return (node_index,
                edge_index,
                edge_labels,
                shared_node_labels,
                shared_node_label_vectors,
                shared_node_cardinalities,
                shared_node_samples,
                shared_node_predicates,
                shared_node_predicate_index,
                my_batch)

    def bulk_mscn_encode_cardinality(self, queries: List[SQLQuery]) -> BulkMSCNCardinalityQueryData:
        base_encodings = self._bulk_mscn_encode_base(queries)
        cardinalities = []
        for query in queries:
            cardinality_estimates = query.cardinality_estimates()
            if "true" in cardinality_estimates:
                cardinalities.append(max(cardinality_estimates["true"], 1))
            else:
                cardinalities.append(-1)
        cardinalities = torch.Tensor(cardinalities)
        return BulkMSCNCardinalityQueryData(*base_encodings, cardinalities)

    def bulk_mscn_encode_relation(self, cardinality_relation: CardinalityRelation) -> BulkMSCNRelationQueryData:
        queries = []
        left = [[], []]
        right = [[], []]
        for query in cardinality_relation.left():
            left[0].append(len(queries))
            left[1].append(0)
            queries.append(query)
        for query in cardinality_relation.right():
            right[0].append(len(queries))
            right[1].append(0)
            queries.append(query)
        equal = [cardinality_relation.type() == RelationType.EQUAL]

        base_encodings = self._bulk_mscn_encode_base(queries)
        left = LongTensor(np.stack(left))
        right = LongTensor(np.stack(right))
        equal = BoolTensor(equal)

        return BulkMSCNRelationQueryData(*base_encodings, left, right, equal)

    def _encode_mscn_predicates(self, predicates: List[List[Predicate]]) -> List[np.array]:
        attribute_preds = {}
        for disjunction in predicates:
            attribute = None
            # disjunctions of predicates on different attributes cannot be encoded with MSCN as is
            clean_disjunction = []
            for predicate in disjunction:
                if attribute is None:
                    attribute = predicate.attribute()
                    clean_disjunction.append(predicate)
                elif attribute == predicate.attribute():
                    clean_disjunction.append(predicate)
            if attribute not in attribute_preds:
                attribute_preds[attribute] = []
            attribute_preds[attribute].append(clean_disjunction)
        predicate_encodings = []
        for attribute in attribute_preds:
            leftovers = []
            for disjunction in attribute_preds[attribute]:
                positive = []
                comparison_operators = []
                values = []
                for predicate in disjunction:
                    positive.append(predicate.positive())
                    comparison_operators.append(predicate.operator())
                    values.append(predicate.value())
                if len(comparison_operators) == 1 and comparison_operators[0] == OPERATORS["IS"]:
                    agg_positive = positive[0]
                    value_encoding = self._encode_mscn_null()
                    operator = 1
                    predicate_encodings.append(self._encode_mscn_predicate(attribute, operator, value_encoding, agg_positive))
                elif all(co == OPERATORS["LIKE"] for co in comparison_operators):
                    agg_positive = all(positive)
                    assert(all(p == agg_positive for p in positive))
                    value_encoding = self._encode_mscn_like(values)
                    operator = 2
                    predicate_encodings.append(self._encode_mscn_predicate(attribute, operator, value_encoding, agg_positive))
                elif all(co == OPERATORS["ILIKE"] for co in comparison_operators):
                    agg_positive = all(positive)
                    assert(all(p == agg_positive for p in positive))
                    value_encoding = self._encode_mscn_like(values)
                    operator = 3
                    predicate_encodings.append(self._encode_mscn_predicate(attribute, operator, value_encoding, agg_positive))
                elif all(co == OPERATORS["="] for co in comparison_operators) or not isinstance(attribute, NumericAttribute):
                    agg_positive = all(positive)
                    assert(all(p == agg_positive for p in positive))
                    value_encoding = self._encode_mscn_categorical(values)
                    operator = 0
                    predicate_encodings.append(self._encode_mscn_predicate(attribute, operator, value_encoding, agg_positive))
                else:
                    leftovers.append((positive, comparison_operators, values))
            if len(leftovers) > 0:
                value_ranges = [(0, 1)]
                for positve, comparison_operators, values in leftovers:
                    disjunction_ranges = []
                    for p, co, v in zip(positve, comparison_operators, values):
                        if isinstance(v, Decimal):
                            v = float(v)
                        v_norm = (v - attribute.minimum()) / (attribute.maximum() - attribute.minimum())
                        v_norm = min(1, max(0, v_norm))
                        if co == OPERATORS["="]:
                            if p:
                                predicate_range = (v_norm, v_norm)
                            else:
                                predicate_range = (0, 1)
                        elif co == OPERATORS["<"]:
                            if p:
                                predicate_range = (0, v_norm)
                            else:
                                predicate_range = (v_norm, 1)
                        elif co == OPERATORS[">"]:
                            if p:
                                predicate_range = (v_norm, 1)
                            else:
                                predicate_range = (0, v_norm)
                        else:
                            raise NotImplementedError()
                        if len(disjunction_ranges) == 0:
                            disjunction_ranges.append(predicate_range)
                        else:
                            pr_min, pr_max = predicate_range
                            new_disjunction_ranges = []
                            for dr_min, dr_max in disjunction_ranges:
                                if dr_max < pr_min or dr_min > pr_max:
                                    new_disjunction_ranges.append((dr_min, dr_max))
                                else:
                                    pr_min = min(dr_min, pr_min)
                                    pr_max = max(dr_max, pr_max)
                            new_disjunction_ranges.append((pr_min, pr_max))
                            disjunction_ranges = sorted(new_disjunction_ranges)
                    value_ranges = self._merge_range(value_ranges, disjunction_ranges)
                agg_positive = True
                value_encoding = self._encode_mscn_range(value_ranges)
                operator = 4
                predicate_encodings.append(self._encode_mscn_predicate(attribute, operator, value_encoding, agg_positive))
        return predicate_encodings

    def _encode_mscn_predicate(self, attribute: Attribute, operator: int, value_encoding: np.array, positive: bool):
        attribute_encoding = self.encode_attribute(attribute)
        operator_encoding = np.zeros((5,))
        operator_encoding[operator] = 1
        if positive:
            positive_encoding = np.ones((1,))
        else:
            positive_encoding = np.zeros((1,))
        return np.concatenate([attribute_encoding, operator_encoding, value_encoding, positive_encoding])

    def mscn_predicate_size(self):
        return self.attribute_encoding_size() + 5 + self._num_buckets + 1

    def _merge_range(self,
                     value_ranges: List[Tuple[float, float]],
                     disjunction_ranges: List[Tuple[float, float]]
                     ) -> List[Tuple[float, float]]:
        new_value_ranges = []
        dr_iter = iter(disjunction_ranges)
        dr_min, dr_max = next(dr_iter)
        for vr_min, vr_max in value_ranges:
            while dr_max < vr_min:
                dr = next(dr_iter, None)
                if dr is None:
                    return new_value_ranges
                else:
                    dr_min, dr_max = dr
            while dr_min < vr_max:
                new_value_ranges.append((max(dr_min, vr_min), min(dr_min, vr_min)))
                dr = next(dr_iter, None)
                if dr is None:
                    return new_value_ranges
                else:
                    dr_min, dr_max = dr
        return new_value_ranges

    def _encode_mscn_range(self, value_ranges: List[Tuple[float, float]]) -> np.array:
        flat_range = [val for value_range in value_ranges for val in value_range]
        features = np.zeros((self._num_buckets,))
        for i, border in enumerate(flat_range[:self._num_buckets]):
            features[i] = border
        return features

    def _encode_mscn_categorical(self, vals: List[Any]) -> np.array:
        features = np.zeros((self._num_buckets,))
        for val in vals:
            pred_idx = self._deterministic_hash(str(val)) % self._num_buckets
            features[pred_idx] = 1
        return features

    # NULL doesn't need to be encoded, it is already implied by the encoding of the IS-comparison operator
    def _encode_mscn_null(self) -> np.array:
        return np.zeros((self._num_buckets,))

    def _deterministic_hash(self, string: str) -> int:
        return int(hashlib.sha1(str(string).encode("utf-8")).hexdigest(), 16)

    def _encode_mscn_like(self, vals: List[str]) -> np.array:
        features = np.zeros((self._num_buckets,))
        char_buckets = self._num_buckets - 2
        regex_val = vals[0].replace("%", "")

        pred_idx = self._deterministic_hash(regex_val) % char_buckets
        features[pred_idx] = 1

        for v in regex_val:
            pred_idx = self._deterministic_hash(str(v)) % char_buckets
            features[pred_idx] = 1

        for i, v in enumerate(regex_val):
            if i != len(regex_val) - 1:
                pred_idx = self._deterministic_hash(v + regex_val[i + 1]) % char_buckets
                features[pred_idx] = 1

        for i, v in enumerate(regex_val):
            if i < len(regex_val) - 2:
                pred_idx = self._deterministic_hash(v + regex_val[i + 1] + regex_val[i + 2]) % char_buckets
                features[pred_idx] = 1

        features[self._num_buckets - 2]= len(regex_val)

        # regex has num or not feature
        if bool(re.search(r'\d', regex_val)):
            features[self._num_buckets - 1] = 1

        return features

    def reset(self):
        if self._sampler is not None:
            self._sampler.reset()
