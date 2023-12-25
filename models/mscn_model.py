

from __future__ import annotations

from typing import Dict, Any

import hyperopt
from torch import FloatTensor
from torch_geometric.nn import MeanAggregation
from loss.logarithmic_mse import LogarithmicMSE
from models.cardinality_model import CardinalityModel
from query.graphlike_query import GraphlikeQuery
from query_data.cardinality_relation_query_data import CardinalityRelationQueryData
from query_data.graphlike_query_data import GraphlikeQueryData
from encoder.encoder import Encoder
from torch.nn import Module, Linear
import torch
from models.util import build_layer_stack


class MSCNModel(CardinalityModel):
    def __init__(self,
                 encoder: Encoder,
                 hidden_units: int = 256,
                 loss: Module = LogarithmicMSE()) -> None:
        CardinalityModel.__init__(self, encoder,loss)

        self._table_layers, _ = build_layer_stack(encoder.node_encoding_size(), [hidden_units] * 2)
        self._table_aggregation = MeanAggregation()
        self._join_layers, _ = build_layer_stack(encoder.edge_encoding_size(), [hidden_units] * 2)
        self._join_aggregation = MeanAggregation()
        self._predicate_layers, _ = build_layer_stack(encoder.predicate_encoding_size(), [hidden_units])
        self._predicate_aggregation = MeanAggregation()
        self._disjunction_layers, _ = build_layer_stack(hidden_units, [hidden_units])
        self._disjunction_aggregation = MeanAggregation()
        self._final_layers, _ = build_layer_stack(hidden_units * 3, [hidden_units])
        self._final_layer = Linear(hidden_units, 1)

    def encode(self, query: GraphlikeQuery, cardinality: int):
        return self._encoder.encode_cardinality(query, cardinality)

    def forward(self,
                data: GraphlikeQueryData
                ) -> FloatTensor:
        if isinstance(data, CardinalityRelationQueryData):
            batch = data.my_batch
            num_graphs = torch.max(data.my_batch) + 1
        else:
            batch = data.batch
            num_graphs = data.num_graphs

        table_embeddings = data.x
        for layer in self._table_layers:
            table_embeddings = layer(table_embeddings)
        tables_embedding = self._table_aggregation(table_embeddings, batch)

        join_embeddings = data.edge_attr
        for layer in self._join_layers:
            join_embeddings = layer(join_embeddings)
        join_index = torch.index_select(batch, 0, data.edge_index[0, :])
        joins_embedding = self._join_aggregation(join_embeddings, join_index, dim_size=num_graphs)

        predicate_embeddings = data.node_predicates
        for layer in self._predicate_layers:
            predicate_embeddings = layer(predicate_embeddings)
        disjunction_embeddings = self._predicate_aggregation(predicate_embeddings, data.node_disjunction_index, dim_size=data.node_conjunction_index.size()[0])
        for layer in self._disjunction_layers:
            disjunction_embeddings = layer(disjunction_embeddings)
        conjunction_index = torch.index_select(batch, 0, data.node_conjunction_index)
        conjunctions_embedding = self._disjunction_aggregation(disjunction_embeddings, conjunction_index, dim_size=num_graphs)

        query_embedding = torch.concat([tables_embedding, joins_embedding, conjunctions_embedding], dim=1)
        for layer in self._final_layers:
            query_embedding = layer(query_embedding)
        log_cardinality = torch.mean(self._final_layer(query_embedding), 1)
        cardinality = torch.exp(log_cardinality)
        return cardinality

    @staticmethod
    def from_config(encoder: Encoder, config: Dict[str, Any]) -> MSCNModel:
        return MSCNModel(encoder, hidden_units=int(config["hidden_units"]))

    @staticmethod
    def config_space() -> Dict[str, Any]:
        space = {}
        space["hidden_units"] = hyperopt.hp.qloguniform("hidden_units", 3, 7, 1)
        return space