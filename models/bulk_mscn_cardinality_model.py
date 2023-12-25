

from __future__ import annotations

import math
from typing import Dict, Any, List, Optional

import hyperopt
from torch import FloatTensor
from torch.nn.init import kaiming_uniform_
from torch_geometric.nn import MeanAggregation
from loss.logarithmic_mse import LogarithmicMSE
from models.cardinality_model import CardinalityModel
from query.graphlike_query import GraphlikeQuery
from query.predicate import ArbitraryPredicate
from query_data.bulk_mscn_query_data import BulkMSCNQueryData
from encoder.encoder import Encoder
from torch.nn import Module, Linear, Parameter
import torch
from models.util import build_layer_stack, MultiHeadAggregation


class BulkMSCNCardinalityModel(CardinalityModel):
    def __init__(self,
                 encoder: Encoder,
                 hidden_units: int = 256,
                 activation: str = "relu",
                 aggregation: str = "mean",
                 independent_samples: bool = False,
                 independent_sample_size: Optional[int] = None,
                 include_predicates: bool = True,
                 loss: Module = LogarithmicMSE()) -> None:
        CardinalityModel.__init__(self, encoder,loss)

        assert(activation in ["relu", "leakyrelu"])
        if activation == "relu":
            activation = torch.nn.ReLU()
        elif activation == "leakyrelu":
            activation = torch.nn.LeakyReLU()
        assert(aggregation in ["mean", "multihead"])

        self._independent_samples = independent_samples
        if independent_samples:
            assert(independent_sample_size is not None)
            self._independent_sample_size = independent_sample_size
            self._sample_weights = kaiming_uniform_(Parameter(torch.empty((encoder.node_label_encoding_size(), independent_sample_size, encoder.sampler().bitmap_size()))), a=math.sqrt(5))
            self._sample_bias = kaiming_uniform_(Parameter(torch.empty((encoder.node_label_encoding_size(), independent_sample_size))), a=math.sqrt(5))
            table_size = independent_sample_size + 1
        else:
            table_size = encoder.node_encoding_size()
        self._table_layers, _ = build_layer_stack(table_size, [hidden_units] * 2, activation=activation)
        if aggregation == "mean":
            self._table_aggregation = MeanAggregation()
            table_size = hidden_units
        elif aggregation == "multihead":
            self._table_aggregation = MultiHeadAggregation(hidden_units, int(hidden_units / 4), 4, True)
            table_size = self._table_aggregation.output_size()
        else:
            raise NotImplementedError()
        self._join_layers, _ = build_layer_stack(encoder.edge_encoding_size(), [hidden_units] * 2, activation=activation)
        if aggregation == "mean":
            self._join_aggregation = MeanAggregation()
            join_size = hidden_units
        elif aggregation == "multihead":
            self._join_aggregation = MultiHeadAggregation(hidden_units, int(hidden_units / 4), 4, True)
            join_size = self._join_aggregation.output_size()
        else:
            raise NotImplementedError()
        self._include_predicates = include_predicates
        if include_predicates:
            self._predicate_layers, _ = build_layer_stack(encoder.mscn_predicate_size(), [hidden_units] * 2, activation=activation)
            if aggregation == "mean":
                self._predicate_aggregation = MeanAggregation()
                predicate_size = hidden_units
            elif aggregation == "multihead":
                self._predicate_aggregation = MultiHeadAggregation(hidden_units, int(hidden_units / 4), 4, True)
                predicate_size = self._predicate_aggregation.output_size()
            else:
                raise NotImplementedError()
            concat_size = table_size + join_size + predicate_size
        else:
            concat_size = table_size + join_size
        self._final_layers, _ = build_layer_stack(concat_size, [hidden_units], activation=activation)
        self._final_layer = Linear(hidden_units, 1)

    def encode(self, query: GraphlikeQuery, cardinality: int):
        return self._encoder.bulk_mscn_encode_cardinality([query])

    def bulk_encode(self, queries: List[GraphlikeQuery]):
        return [self._encoder.bulk_mscn_encode_cardinality(queries)]

    def forward(self,
                data: BulkMSCNQueryData
                ) -> FloatTensor:
        num_queries = data.my_batch[-1] + 1

        if self._independent_samples:
            sample_weights = torch.index_select(self._sample_weights, 0, data.shared_node_labels)
            sample_biases = torch.index_select(self._sample_bias, 0, data.shared_node_labels)
            sample_embedding = torch.matmul(sample_weights, data.shared_node_samples.unsqueeze(dim=2)).squeeze() + sample_biases
            shared_table_embeddings = torch.cat([sample_embedding, data.shared_node_cardinalities.unsqueeze(dim=1)], dim=1)
        else:
            shared_table_embeddings = torch.cat([data.shared_node_label_vectors, data.shared_node_samples, data.shared_node_cardinalities.unsqueeze(dim=1)], dim=1)
        for layer in self._table_layers:
            shared_table_embeddings = layer(shared_table_embeddings)
        table_embeddings = torch.index_select(shared_table_embeddings, 0, data.x)
        table_aggregates = self._table_aggregation(table_embeddings, data.my_batch, dim_size=num_queries)

        join_embeddings = data.edge_attr
        for layer in self._join_layers:
            join_embeddings = layer(join_embeddings)
        join_batch = torch.index_select(data.my_batch, 0, data.edge_index[0])
        join_aggregates = self._join_aggregation(join_embeddings, join_batch, dim_size=num_queries)

        if self._include_predicates:
            shared_predicate_embeddings = data.shared_node_predicates
            for layer in self._predicate_layers:
                shared_predicate_embeddings = layer(shared_predicate_embeddings)
            predicate_embeddings = torch.index_select(shared_predicate_embeddings, 0, data.shared_node_predicate_index[0])
            predicate_aggregates = self._predicate_aggregation(predicate_embeddings, data.shared_node_predicate_index[1], dim_size=num_queries)
            query_embedding = torch.concat([table_aggregates, join_aggregates, predicate_aggregates], dim=1)
        else:
            query_embedding = torch.concat([table_aggregates, join_aggregates], dim=1)

        for layer in self._final_layers:
            query_embedding = layer(query_embedding)
        log_cardinality = torch.flatten(self._final_layer(query_embedding))
        cardinality = torch.exp(log_cardinality)
        return cardinality

    @staticmethod
    def from_config(encoder: Encoder, config: Dict[str, Any]) -> BulkMSCNCardinalityModel:
        return BulkMSCNCardinalityModel(encoder,
                                        hidden_units=int(config["hidden_units"]),
                                        activation=config["activation"],
                                        aggregation=config["aggregation"],
                                        independent_samples=config["independent_samples"],
                                        independent_sample_size=config["independent_sample_size"],
                                        include_predicates=config["include_predicates"])

    @staticmethod
    def config_space() -> Dict[str, Any]:
        space = {}
        space["hidden_units"] = hyperopt.hp.qloguniform("hidden_units", 3, 7, 1)
        return space

    @staticmethod
    def supports(query: GraphlikeQuery) -> bool:
        for node in query.nodes():
            for disjunction in node.predicates():
                for predicate in disjunction:
                    if isinstance(predicate, ArbitraryPredicate):
                        return False
        return True
