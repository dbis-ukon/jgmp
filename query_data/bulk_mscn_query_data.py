import torch
from torch import FloatTensor, LongTensor, BoolTensor
from torch_geometric.data import Data


class BulkMSCNQueryData(Data):
    def __init__(self,
                 node_index: LongTensor,
                 edge_index: LongTensor,
                 edge_labels: FloatTensor,
                 shared_node_labels: LongTensor,
                 shared_node_label_vectors: FloatTensor,
                 shared_node_cardinalities: FloatTensor,
                 shared_node_samples: FloatTensor,
                 shared_node_predicates: FloatTensor,
                 shared_node_predicate_index: LongTensor,
                 my_batch: LongTensor) -> None:
        super(BulkMSCNQueryData, self).__init__(node_index, edge_index, edge_labels)
        self.shared_node_labels = shared_node_labels
        self.shared_node_label_vectors = shared_node_label_vectors
        self.shared_node_cardinalities = shared_node_cardinalities
        self.shared_node_samples = shared_node_samples
        self.shared_node_predicates = shared_node_predicates
        self.shared_node_predicate_index = shared_node_predicate_index
        self.my_batch = my_batch

    def __inc__(self, key, value, *args, **kwargs):
        if key == "x":
            return self.shared_node_labels.size()[0]
        elif key == "shared_node_predicate_index":
            return torch.tensor([[self.shared_node_predicates.size()[0]], [self.my_batch[-1] + 1]])
        elif key == "my_batch":
            return self.my_batch[-1] + 1
        else:
            return super().__inc__(key, value)

    def __cat_dim__(self, key: str, value, *args, **kwargs):
        if key == "shared_node_predicate_index":
            return 1
        else:
            return super().__cat_dim__(key, value, *args, **kwargs)
