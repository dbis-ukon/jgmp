

from __future__ import annotations

import math
from torch.nn.functional import leaky_relu
from torch.nn.init import kaiming_uniform_
from encoder.encoder import Encoder
from loss.logarithmic_mse import LogarithmicMSE
from models.cardinality_model import CardinalityModel
from models.multi_head_attention_conv_index import MultiHeadAttentionConvIndex
from models.util import build_layer_stack, MultiHeadAggregation
from query.graphlike_query import GraphlikeQuery
from query_data.bulk_light_query_data import BulkLightQueryData
from typing import List, Tuple, Any, Dict
from torch.nn.modules.linear import Linear
from torch.nn import Module, Parameter, ModuleList
import torch



class BulkJGMPCardinalityModel(CardinalityModel):
    def __init__(self,
                 encoder: Encoder,
                 loss: Module = LogarithmicMSE(),
                 sample_layer_size: int = 32,
                 node_layer_sizes: List[int] = [],
                 edge_layer_sizes: List[int] = [8],
                 graph_layer_sizes: List[Tuple[int, int]] = [(8, 4), (8, 4), (8, 4)],
                 skip_connections: bool = True,
                 node_aggregate_head_size: int = 128,
                 node_aggregate_heads: int = 8,
                 final_layer_sizes: List[int] = [256],
                 use_pg_estimates: bool = True,
                 encode_fk_direction: bool = True
                 ):
        CardinalityModel.__init__(self, encoder, loss)
        self._use_pg_estimates = use_pg_estimates
        self._encode_fk_direction = encode_fk_direction

        self._sample_weights = kaiming_uniform_(Parameter(torch.empty((encoder.node_label_encoding_size(), sample_layer_size, encoder.sampler().bitmap_size()))), a=math.sqrt(5))
        self._sample_bias = kaiming_uniform_(Parameter(torch.empty((encoder.node_label_encoding_size(), sample_layer_size))), a=math.sqrt(5))
        if use_pg_estimates:
            node_size = sample_layer_size + 1
        else:
            node_size = sample_layer_size
        self._node_layers, node_size = build_layer_stack(node_size, node_layer_sizes)

        if encode_fk_direction:
            edge_size = encoder.edge_encoding_size() + 1
        else:
            edge_size = 2 * encoder.edge_encoding_size()
        self._edge_layers, edge_size = build_layer_stack(edge_size, edge_layer_sizes)

        self._graph_layers = ModuleList()
        node_sizes = [node_size]
        for size_per_tower, towers in graph_layer_sizes:
            size = size_per_tower * towers
            self._graph_layers.append(MultiHeadAttentionConvIndex(node_size, edge_size, size, node_size, towers, size_per_tower))
            node_size = size
            node_sizes.append(node_size)

        self._skip_connections = skip_connections
        if skip_connections:
            final_node_size = sum(node_sizes)
        else:
            final_node_size = node_size

        self._node_aggregator = MultiHeadAggregation(final_node_size, node_aggregate_head_size, node_aggregate_heads, True)
        graph_size = self._node_aggregator.output_size()

        self._final_layers, graph_size = build_layer_stack(graph_size, final_layer_sizes)
        self._final_layers.append(Linear(graph_size, 1))

    def encode(self, query: GraphlikeQuery, cardinality: int):
        return self._encoder.bulk_light_encode_cardinality([query])

    def bulk_encode(self, queries: List[GraphlikeQuery]):
        return [self._encoder.bulk_light_encode_cardinality(queries)]

    def forward(self, data: BulkLightQueryData):
        # retrieve sample_weights based on the label of the table
        # sample bitmaps between different tables are not semantically related so they should be processed by different sets of weights
        sample_weights = torch.index_select(self._sample_weights, 0, data.shared_node_labels)
        sample_biases = torch.index_select(self._sample_bias, 0, data.shared_node_labels)
        samples = torch.matmul(sample_weights, data.shared_node_samples.unsqueeze(dim=2)).squeeze() + sample_biases

        if self._use_pg_estimates:
            # append pg estimates to samples
            shared_nodes = torch.cat([samples, data.shared_node_cardinalities.unsqueeze(dim=1)], dim=1)
        else:
            shared_nodes = samples

        for layer in self._node_layers:
            shared_nodes = layer(shared_nodes)
        # steps until here were the same for copies of the same node (table instance) between different subplan-queries
        # now we make a copy of the shared table embeddings for each copy of the same node
        # this allows us to compute different node embeddings based on the structure of the subplan-queries
        nodes = torch.index_select(shared_nodes, 0, data.x)

        if self._encode_fk_direction:
            natural_edges = torch.nn.functional.pad(data.edge_attr, (0,1), "constant", 1)
            reversed_edges = torch.nn.functional.pad(data.edge_attr, (0,1), "constant", 0)
        else:
            padding = torch.zeros_like(data.edge_attr)
            natural_edges = torch.cat([data.edge_attr, padding], dim=1)
            reversed_edges = torch.cat([padding, data.edge_attr], dim=1)
        for layer in self._edge_layers:
            natural_edges = layer(natural_edges)
            reversed_edges = layer(reversed_edges)
        edges = torch.cat([natural_edges, reversed_edges])
        edge_index = torch.cat([data.edge_index, torch.flip(data.edge_index, [0])], dim=1)

        skip_nodes = [nodes]
        for layer in self._graph_layers:
            nodes = layer(nodes, edge_index, edges)
            skip_nodes.append(nodes)
        if self._skip_connections:
            nodes = torch.cat(skip_nodes, 1)
        graphs = self._node_aggregator(nodes, data.my_batch)

        for layer in self._final_layers:
            graphs = layer(graphs)
        return torch.exp(torch.flatten(graphs))

    @staticmethod
    def from_config(encoder: Encoder, config: Dict[str, Any]) -> BulkJGMPCardinalityModel:
        if "use_pg_estimates" not in config:
            config["use_pg_estimates"] = True
        if "encode_fk_direction" not in config:
            config["encode_fk_direction"] = True
        return BulkJGMPCardinalityModel(encoder,
                                        sample_layer_size=int(config["sample_layer_size"]),
                                        node_layer_sizes=[int(config["node_size"])] * config["node_number"],
                                        edge_layer_sizes=[int(config["edge_size"])] * config["edge_number"],
                                        graph_layer_sizes=[(int(config["size_per_tower"]), int(config["towers"]))] * config["conv_number"],
                                        skip_connections=config["skip_connections"],
                                        node_aggregate_head_size=int(config["node_aggregate_head_size"]),
                                        node_aggregate_heads=int(config["node_aggregate_heads"]),
                                        final_layer_sizes=[int(config["final_size"])] * config["final_number"],
                                        use_pg_estimates=config["use_pg_estimates"],
                                        encode_fk_direction=config["encode_fk_direction"])

    @staticmethod
    def config_space() -> Dict[str, Any]:
        raise NotImplementedError()

    def pad_sample_weights(self, new_bitmap_size: int):
        self._sample_weights = Parameter(torch.nn.functional.pad(self._sample_weights, (0, new_bitmap_size - self._sample_weights.size(2)), "constant", 0))
