

from __future__ import annotations

from torch import FloatTensor
from torch.nn.functional import leaky_relu

from encoder.encoder import Encoder
from loss.logarithmic_mse import LogarithmicMSE
from models.cardinality_model import CardinalityModel
from models.query_model import QueryModel
from models.util import build_layer_stack
from query.graphlike_query import GraphlikeQuery
from query_data.graphlike_query_data import GraphlikeQueryData
from typing import List
from torch.nn.modules.linear import Linear
from torch.nn import Module
import torch


class GraphCardinalityModel(CardinalityModel):
    def __init__(self,
                 encoder: Encoder,
                 query_model: QueryModel,
                 loss: Module = LogarithmicMSE(),
                 final_layer_sizes: List[int] = [16]) -> None:
        CardinalityModel.__init__(self, encoder, loss)

        self._query_model = query_model

        graph_size = query_model.graph_out_size()
        self._final_layers, graph_size = build_layer_stack(graph_size, final_layer_sizes)

        self._out_layer = Linear(graph_size, 1)

    def encode(self, query: GraphlikeQuery, cardinality: int):
        return self._encoder.encode_cardinality(query, cardinality)

    def forward(self, data: GraphlikeQueryData) -> FloatTensor:
        query_vec = self._query_model.forward(data)
        for layer in self._final_layers:
            query_vec = layer(query_vec)
            query_vec = leaky_relu(query_vec)

        output = torch.mean(self._out_layer(query_vec), 1)
        output = torch.exp(output)
        return output

    @staticmethod
    def from_config(encoder: Encoder, config: dict) -> GraphCardinalityModel:
        query_model = QueryModel.from_config(encoder, config)
        return GraphCardinalityModel(encoder, query_model, final_layer_sizes=[])
