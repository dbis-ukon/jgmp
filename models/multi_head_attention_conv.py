import torch
from torch import Tensor
from torch.nn import Linear
from torch.nn.functional import leaky_relu
from torch_geometric.nn import MessagePassing
from torch_geometric.typing import Adj

from models.util import MultiHeadAggregation


class MultiHeadAttentionConv(MessagePassing):
    def __init__(self,
                 in_channels: int,
                 edge_channels: int,
                 out_channels: int,
                 pre_size: int,
                 num_heads: int,
                 head_size: int,
                 count: bool = True):
        aggr = MultiHeadAggregation(pre_size, head_size, num_heads, count=count)
        super().__init__(aggr=aggr, node_dim=0)

        self._pre_layer = Linear(in_channels + edge_channels, pre_size)
        self._out_layer = Linear(in_channels + aggr.output_size(), out_channels)

    def forward(self,
                x: Tensor,
                edge_index: Adj,
                edge_attr: Tensor = None) -> Tensor:
        out = self.propagate(edge_index, x=x, edge_attr=edge_attr, size=None)
        out = torch.cat([x, out], dim=-1)
        out = self._out_layer(out)
        out = leaky_relu(out)
        return out

    def message(self,
                x_j: Tensor,
                edge_attr: Tensor) -> Tensor:
        message = torch.cat([x_j, edge_attr], dim=-1)
        message = self._pre_layer(message)
        message = leaky_relu(message)
        return message

