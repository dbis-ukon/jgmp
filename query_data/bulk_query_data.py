
from torch import FloatTensor, LongTensor, BoolTensor
from torch_geometric.data import Data


class BulkQueryData(Data):
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
                 my_batch: LongTensor) -> None:
        super(BulkQueryData, self).__init__(node_index, edge_index, edge_labels)
        self.shared_node_labels = shared_node_labels
        self.shared_node_cardinalities = shared_node_cardinalities
        self.shared_node_samples = shared_node_samples
        self.shared_node_predicates = shared_node_predicates
        self.shared_node_disjunction_index = shared_node_disjunction_index
        self.shared_node_conjunction_index = shared_node_conjunction_index
        self.my_batch = my_batch

    def __inc__(self, key, value, *args, **kwargs):
        if key == "x":
            return self.shared_node_labels.size()[0]
        elif key == "shared_node_disjunction_index":
            return self.shared_node_conjunction_index.size()[0]
        elif key == "shared_node_conjunction_index":
            return self.shared_node_labels.size()[0]
        elif key == "my_batch":
            return self.my_batch[-1] + 1
        else:
            return super().__inc__(key, value)
