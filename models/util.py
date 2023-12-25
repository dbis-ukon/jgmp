from enum import Enum
import torch
import torch_scatter
from torch import FloatTensor, LongTensor, Tensor
from typing import List, Tuple, Optional

from torch.nn import Linear, Module, ModuleList
from torch_geometric.nn import AttentionalAggregation, SumAggregation, MaxAggregation, Aggregation


class AggregationType(Enum):
    ATTENTION = 1
    SUM = 2
    MAX = 3


class ResizeAggregation(Aggregation):
    def __init__(self, input_size: int, output_size: int, aggregator: Aggregation):
        super(ResizeAggregation, self).__init__()
        self._resize = Linear(input_size, output_size)
        self._aggregator = aggregator

    def forward(self,
                x: Tensor,
                index: Optional[Tensor] = None,
                ptr: Optional[Tensor] = None,
                dim_size: Optional[int] = None,
                dim: int = -2) -> Tensor:
        x = self._resize(x)
        return self._aggregator.forward(x, index=index, ptr=ptr, dim_size=dim_size, dim=dim)


class MultiHeadAggregation(Aggregation):
    def __init__(self, input_size: int, output_size_per_head: int, num_heads: int, count: bool):
        super(MultiHeadAggregation, self).__init__()
        attention_size = output_size_per_head * num_heads
        if count:
            self._output_size = attention_size + 1
        else:
            self._output_size = attention_size
        self._count = count
        self._aggregators = ModuleList()
        for i in range(num_heads):
            self._aggregators.append(AttentionalAggregation(Linear(input_size, 1), nn=Linear(input_size, output_size_per_head)))

    def output_size(self):
        return self._output_size

    def forward(self,
                x: Tensor,
                index: Optional[Tensor] = None,
                ptr: Optional[Tensor] = None,
                dim_size: Optional[int] = None,
                dim: int = -2) -> Tensor:
        aggregations = []
        for aggregator in self._aggregators:
            aggregations.append(aggregator.forward(x, index=index, ptr=ptr, dim_size=dim_size, dim=dim))
        if self._count:
            ones = torch.ones_like(index)
            count = torch_scatter.scatter(ones, index, dim=0, dim_size=dim_size, reduce="sum").unsqueeze(dim=1)
            aggregations.append(count)
        return torch.cat(aggregations, dim=1)


def build_aggregation(aggregation_type: AggregationType, input_size: int, output_size: int) -> Module:
    if aggregation_type == AggregationType.ATTENTION:
        return AttentionalAggregation(Linear(input_size, 1), nn=Linear(input_size, output_size))
    elif aggregation_type == AggregationType.SUM:
        return ResizeAggregation(input_size, output_size, SumAggregation())
    elif aggregation_type == AggregationType.MAX:
        return ResizeAggregation(input_size, output_size, MaxAggregation())


def build_layer_stack(previous: int,
                      layers: List[int],
                      dropout: float = 0,
                      activation: torch.nn.Module = torch.nn.LeakyReLU()
                      ) -> Tuple[torch.nn.ModuleList, int]:
    modules = torch.nn.ModuleList()
    for layer in layers:
        modules.append(torch.nn.Linear(previous, layer))
        modules.append(activation)
        if dropout > 0:
            modules.append(torch.nn.Dropout(dropout))
        previous = layer
    return modules, previous


def aggregate_many_to_many(input_tensor: FloatTensor, index: LongTensor, dim_size: int) -> FloatTensor:
    gathered = torch.index_select(input_tensor, 0, index[0])
    scattered = torch_scatter.scatter(gathered, index[1], dim=0, dim_size=dim_size)
    return scattered
