

from typing import Any
from torch import FloatTensor, LongTensor, BoolTensor
import torch
from query_data.graphlike_query_data import GraphlikeQueryData


class CardinalityRelationQueryData(GraphlikeQueryData):
    def __init__(self,
                 node_labels: FloatTensor,
                 edge_index: LongTensor,
                 edge_labels: FloatTensor,
                 edge_directions: FloatTensor,
                 edge_directions_reversed: FloatTensor,
                 node_predicates: FloatTensor,
                 node_disjunction_index: LongTensor,
                 node_conjunction_index: LongTensor,
                 edge_predicates: FloatTensor,
                 edge_disjunction_index: LongTensor,
                 edge_conjunction_index: LongTensor,
                 node_count: FloatTensor,
                 left: LongTensor,
                 right: LongTensor,
                 equal: BoolTensor,
                 my_batch: LongTensor):
        super().__init__(node_labels,
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
                         node_count)
        self.left = left
        self.right = right
        self.equal = equal
        self.my_batch = my_batch

    def __inc__(self, key: str, value: Any, *args, **kwargs):
        if key == "left" or key == "right":
            return torch.tensor([[self.my_batch[-1] + 1], [self.equal.size(0)]])
        elif key == "my_batch":
            return self.my_batch[-1] + 1
        else:
            return super().__inc__(key, value)

    def __cat_dim__(self, key: str, value, *args, **kwargs):
        if key == "left" or key == "right":
            return 1
        else:
            return super().__cat_dim__(key, value, *args, **kwargs)
