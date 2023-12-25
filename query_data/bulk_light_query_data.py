
from torch import FloatTensor, LongTensor, BoolTensor
from torch_geometric.data import Data


class BulkLightQueryData(Data):
    def __init__(self,
                 node_index: LongTensor,
                 edge_index: LongTensor,
                 edge_labels: FloatTensor,
                 shared_node_labels: LongTensor,
                 shared_node_cardinalities: FloatTensor,
                 shared_node_samples: FloatTensor,
                 my_batch: LongTensor) -> None:
        super(BulkLightQueryData, self).__init__(node_index, edge_index, edge_labels)
        self.shared_node_labels = shared_node_labels
        self.shared_node_cardinalities = shared_node_cardinalities
        self.shared_node_samples = shared_node_samples
        self.my_batch = my_batch

    def __inc__(self, key, value, *args, **kwargs):
        if key == "x":
            return self.shared_node_labels.size()[0]
        elif key == "my_batch":
            return self.my_batch[-1] + 1
        else:
            return super().__inc__(key, value)
