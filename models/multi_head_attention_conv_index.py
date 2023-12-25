import torch
from torch import Tensor
from torch.nn import Linear, Module
from torch.nn.functional import leaky_relu
from torch_geometric.typing import Adj

from models.util import MultiHeadAggregation


class MultiHeadAttentionConvIndex(Module):
    def __init__(self,
                 in_channels: int,
                 edge_channels: int,
                 out_channels: int,
                 pre_size: int,
                 num_heads: int,
                 head_size: int,
                 count: bool = True):
        super().__init__()
        self._aggr = MultiHeadAggregation(pre_size, head_size, num_heads, count=count)
        self._pre_layer = Linear(in_channels + edge_channels, pre_size)
        self._out_layer = Linear(in_channels + self._aggr.output_size(), out_channels)

    def forward(self,
                x: Tensor,
                edge_index: Adj,
                edge_attr: Tensor = None) -> Tensor:
        x_j = torch.index_select(x, 0, edge_index[0])
        message = torch.cat([x_j, edge_attr], dim=-1)
        message = self._pre_layer(message)
        message = leaky_relu(message)
        messages = self._aggr(message, edge_index[1], dim_size=x.size()[0])
        out = torch.cat([x, messages], dim=-1)
        out = self._out_layer(out)
        out = leaky_relu(out)
        return out
