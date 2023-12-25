from typing import Any

import torch
from torch import FloatTensor, LongTensor, BoolTensor
from query_data.bulk_query_data import BulkQueryData


class BulkRelationQueryData(BulkQueryData):
    def __init__(self,
                 node_index: LongTensor,
                 edge_index: LongTensor,
                 edge_labels: FloatTensor,
                 shared_node_labels: LongTensor,
                 shared_node_cardinalities: FloatTensor,
                 shared_node_samples: FloatTensor,
                 shared_node_predicates: FloatTensor,
                 shared_node_disjunction_index: LongTensor,
                 shared_node_conjunction_index: LongTensor,
                 my_batch: LongTensor,
                 left: LongTensor,
                 right: LongTensor,
                 equal: BoolTensor) -> None:
        super(BulkRelationQueryData, self).__init__(node_index,
                                                    edge_index,
                                                    edge_labels,
                                                    shared_node_labels,
                                                    shared_node_cardinalities,
                                                    shared_node_samples,
                                                    shared_node_predicates,
                                                    shared_node_disjunction_index,
                                                    shared_node_conjunction_index,
                                                    my_batch)
        self.left = left
        self.right = right
        self.equal = equal

    def __inc__(self, key: str, value: Any, *args, **kwargs):
        if key == "left" or key == "right":
            return torch.tensor([[self.my_batch[-1] + 1], [self.equal.size(0)]])
        else:
            return super().__inc__(key, value)

    def __cat_dim__(self, key: str, value, *args, **kwargs):
        if key == "left" or key == "right":
            return 1
        else:
            return super().__cat_dim__(key, value, *args, **kwargs)