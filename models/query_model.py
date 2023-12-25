

from __future__ import annotations
from enum import Enum
from torch import LongTensor, FloatTensor
from torch.nn.functional import leaky_relu
from torch_geometric.nn.conv.gen_conv import GENConv
from query_data.cardinality_relation_query_data import CardinalityRelationQueryData
from query_data.graphlike_query_data import GraphlikeQueryData
from typing import Any, Dict, List, Optional, Tuple
from torch.nn.parameter import Parameter
from encoder.encoder import Encoder
from torch.nn import Module, ModuleList
import torch
import hyperopt
import numpy as np
from models.util import build_layer_stack, AggregationType, build_aggregation


class ModelType(Enum):
    SQL = 1
    CYPHER = 2


class QueryModel(Module):
    def __init__(self,
                 encoder: Encoder,
                 predicate_layer_sizes: List[int] = [64],
                 predicate_aggregate_size: int = 64,
                 predicate_aggregation_type: AggregationType = AggregationType.ATTENTION,
                 disjunction_layer_sizes: List[int] = [64],
                 disjunction_aggregate_size: int = 128,
                 disjunction_aggregation_type: AggregationType = AggregationType.ATTENTION,
                 node_layer_sizes: List[int] = [],
                 edge_layer_sizes: List[int] = [],
                 graph_layer_size: int = 256,
                 graph_layer_number: int = 1,
                 share_between_graph_convolutions: bool = True,
                 node_aggregate_size: int = 512,
                 node_aggregation_type: AggregationType = AggregationType.ATTENTION,
                 aggregate_all: bool = True,
                 final_layer_sizes: List[int] = [256, 64],
                 dropout: float = 0,
                 model_type: ModelType = ModelType.SQL) -> None:
        super().__init__()
        self._model_type = model_type
        self._aggregate_all = aggregate_all

        predicate_size = encoder.predicate_encoding_size()
        self._predicate_layers, predicate_size = build_layer_stack(predicate_size, predicate_layer_sizes, dropout=dropout)
        self._predicate_aggregator = build_aggregation(predicate_aggregation_type, predicate_size, predicate_aggregate_size)

        disjunction_size = predicate_aggregate_size
        self._disjunction_layers, disjunction_size = build_layer_stack(disjunction_size, disjunction_layer_sizes, dropout=dropout)
        self._disjunction_aggregator = build_aggregation(disjunction_aggregation_type, disjunction_size, disjunction_aggregate_size)
        self._predicate_out_size = disjunction_aggregate_size

        node_layer_sizes.append(graph_layer_size)
        node_size = disjunction_aggregate_size + encoder.node_encoding_size()
        self._node_layers, node_size = build_layer_stack(node_size, node_layer_sizes, dropout=dropout)

        edge_layer_sizes.append(graph_layer_size)
        edge_size = encoder.edge_encoding_size() + encoder.edge_direction_size()
        if self._model_type == ModelType.CYPHER:
            edge_size += disjunction_aggregate_size
        self._edge_layers, edge_size = build_layer_stack(edge_size, edge_layer_sizes, dropout=dropout)

        self._edge_out_size = edge_size

        if aggregate_all:
            node_aggregate_sizes = node_aggregate_size // (graph_layer_number + 1)
            graph_size = node_aggregate_sizes * (graph_layer_number + 1) + 1
        else:
            node_aggregate_sizes = node_aggregate_size
            graph_size = node_aggregate_size + 1

        self._graph_layers = ModuleList()
        self._node_aggregators = ModuleList()
        shared_conv = GENConv(graph_layer_size, graph_layer_size, norm=None)
        for _ in range(graph_layer_number):
            if aggregate_all:
                self._node_aggregators.append(build_aggregation(node_aggregation_type, graph_layer_size, node_aggregate_sizes))
            if share_between_graph_convolutions:
                self._graph_layers.append(shared_conv)
            else:
                self._graph_layers.append(GENConv(graph_layer_size, graph_layer_size, norm=None))

        self._node_aggregators.append(build_aggregation(node_aggregation_type, graph_layer_size, node_aggregate_sizes))

        self._node_out_size = graph_layer_size

        self._final_layers, graph_size = build_layer_stack(graph_size, final_layer_sizes, dropout=dropout)
        self._graph_out_size = graph_size

        self._swap_tensor = Parameter(LongTensor([1, 0]), requires_grad=False)

    def predicate_out_size(self) -> int:
        return self._predicate_out_size

    def edge_out_size(self) -> int:
        return self._edge_out_size

    def node_out_size(self) -> int:
        return self._node_out_size

    def graph_out_size(self) -> int:
        return self._graph_out_size

    def _forward_predicates(self,
                            predicate_vec: FloatTensor,
                            disjunction_index: LongTensor,
                            conjunction_index: LongTensor,
                            num_disjunctions: int,
                            num_predicatable: int
                            ) -> FloatTensor:
        for layer in self._predicate_layers:
            predicate_vec = layer(predicate_vec)

        disjunction_vec = self._predicate_aggregator(predicate_vec, disjunction_index, dim_size=num_disjunctions)
        for layer in self._disjunction_layers:
            disjunction_vec = layer(disjunction_vec)

        return self._disjunction_aggregator(disjunction_vec, conjunction_index, dim_size=num_predicatable)

    def _forward_base_nodes(self,
                            data: GraphlikeQueryData,
                            additional_node_attributes: Optional[FloatTensor]
                            ) -> FloatTensor:
        node_predicates_vec = self._forward_predicates(data.node_predicates,
                                                       data.node_disjunction_index,
                                                       data.node_conjunction_index,
                                                       data.node_conjunction_index.size()[0],
                                                       data.num_nodes)
        if additional_node_attributes is not None:
            node_predicates_vec = torch.add(node_predicates_vec, additional_node_attributes)
        node_vec = torch.cat([data.x, node_predicates_vec], dim=1)
        for layer in self._node_layers:
            node_vec = layer(node_vec)
        return node_vec

    def _forward_base_edges(self,
                            data: GraphlikeQueryData
                            ) -> Tuple[FloatTensor, FloatTensor, LongTensor, LongTensor]:
        if self._model_type == ModelType.SQL:
            edge_vec = data.edge_attr
        elif self._model_type == ModelType.CYPHER:
            edge_predicates_vec = self._forward_predicates(data.edge_predicates,
                                                           data.edge_disjunction_index,
                                                           data.edge_conjunction_index,
                                                           data.edge_conjunction_index.size()[0],
                                                           data.num_edges)
            edge_vec = torch.cat([data.edge_attr, edge_predicates_vec], dim=1)
        else:
            raise NotImplementedError()
        edge_vec_natural = torch.cat([edge_vec, data.edge_directions], dim=1)
        edge_vec_reversed = torch.cat([edge_vec, data.edge_directions_reversed], dim=1)
        edge_index = data.edge_index
        edge_index_reversed = data.edge_index.index_select(0, self._swap_tensor)

        for layer in self._edge_layers:
            edge_vec_natural = layer(edge_vec_natural)
            edge_vec_reversed = layer(edge_vec_reversed)
        return edge_vec_natural, edge_vec_reversed, edge_index, edge_index_reversed

    def forward_complete(self,
                         data: GraphlikeQueryData,
                         additional_node_attributes: Optional[FloatTensor] = None
                         ) -> Tuple[FloatTensor, FloatTensor, FloatTensor, FloatTensor]:
        if isinstance(data, CardinalityRelationQueryData):
            batch = data.my_batch
        else:
            batch = data.batch

        node_vec = self._forward_base_nodes(data, additional_node_attributes)

        edge_vec1, edge_vec2, edge_index1, edge_index2 = self._forward_base_edges(data)

        edge_vec = torch.cat([edge_vec1, edge_vec2], dim=0)
        edge_index = torch.cat([edge_index1, edge_index2], dim=1)

        query_vecs = []
        for i, layer in enumerate(self._graph_layers):
            if self._aggregate_all:
                query_vecs.append(self._node_aggregators[i](node_vec, batch))
            node_vec = layer(node_vec, edge_index, edge_attr=edge_vec)
            node_vec = leaky_relu(node_vec)

        query_vecs.append(self._node_aggregators[-1](node_vec, batch))
        query_vecs.append(data.node_count)

        query_vec = torch.cat(query_vecs, dim=1)
        for layer in self._final_layers:
            query_vec = layer(query_vec)

        return query_vec, node_vec, edge_vec1, edge_vec2

    def forward(self,
                data: GraphlikeQueryData,
                additional_node_attributes: Optional[FloatTensor] = None
                ) -> FloatTensor:
        return self.forward_complete(data, additional_node_attributes=additional_node_attributes)[0]

    @staticmethod
    def from_config(encoder: Encoder, config: Dict[str, Any]) -> QueryModel:
        total_size = int(config["node_aggregate_size"])
        final_layer_sizes = [int(total_size * np.exp(- i * np.log(total_size) / config["final_number"])) for i in range(config["final_number"])]
        return QueryModel(encoder,
                          predicate_aggregate_size=int(config["predicate_aggregate_size"]),
                          predicate_aggregation_type=config["predicate_aggregation_type"],
                          disjunction_aggregate_size=int(config["disjunction_aggregate_size"]),
                          disjunction_aggregation_type=config["disjunction_aggregation_type"],
                          node_aggregate_size=int(config["node_aggregate_size"]),
                          node_aggregation_type=config["node_aggregation_type"],
                          predicate_layer_sizes=[int(config["predicate_size"])] * config["predicate_number"],
                          disjunction_layer_sizes=[int(config["disjunction_size"])] * config["disjunction_number"],
                          node_layer_sizes=[int(config["node_size"])] * config["node_number"],
                          edge_layer_sizes=[int(config["edge_size"])] * config["edge_number"],
                          graph_layer_size=int(config["conv_size"]),
                          graph_layer_number=config["conv_number"],
                          share_between_graph_convolutions=config["share_conv"],
                          aggregate_all=config["aggregate_all"],
                          final_layer_sizes=final_layer_sizes)

    @staticmethod
    def config_space() -> Dict[str, Any]:
        space = {}
        space["predicate_aggregate_size"] = hyperopt.hp.qloguniform("predicate_aggregate_size", 2, 6, 1)
        space["disjunction_aggregate_size"] = hyperopt.hp.qloguniform("disjunction_aggregate_size", 3, 7, 1)
        space["node_aggregate_size"] = hyperopt.hp.qloguniform("node_aggregate_size", 3, 8, 1)

        aggregation_types = [AggregationType.ATTENTION, AggregationType.SUM, AggregationType.MAX]
        space["predicate_aggregation_type"] = hyperopt.hp.choice("predicate_aggregation_type", aggregation_types)
        space["disjunction_aggregation_type"] = hyperopt.hp.choice("disjunction_aggregation_type", aggregation_types)
        space["node_aggregation_type"] = hyperopt.hp.choice("node_aggregation_type", aggregation_types)

        space["predicate_size"] = hyperopt.hp.qloguniform("predicate_size", 1, 5, 1)
        space["predicate_number"] = hyperopt.hp.randint("predicate_number", 3)

        space["disjunction_size"] = hyperopt.hp.qloguniform("disjunction_size", 2, 6, 1)
        space["disjunction_number"] = hyperopt.hp.randint("disjunction_number", 3)

        space["node_size"] = hyperopt.hp.qloguniform("node_size", 3, 7, 1)
        space["node_number"] = hyperopt.hp.randint("node_number", 3)

        space["edge_size"] = hyperopt.hp.qloguniform("edge_size", 3, 7, 1)
        space["edge_number"] = hyperopt.hp.randint("edge_number", 3)

        space["conv_size"] = hyperopt.hp.qloguniform("conv_size", 3, 7, 1)
        space["conv_number"] = hyperopt.hp.randint("conv_number", 6)

        space["share_conv"] = hyperopt.hp.choice("share_conv", [True, False])
        space["aggregate_all"] = hyperopt.hp.choice("aggregate_all", [True, False])

        space["final_number"] = hyperopt.hp.randint("final_number", 4)
        return space
