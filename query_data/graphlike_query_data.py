
from torch import FloatTensor, LongTensor
from torch_geometric.data import Data


class GraphlikeQueryData(Data):
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
                 node_count: FloatTensor) -> None:
        super(GraphlikeQueryData, self).__init__(node_labels, edge_index, edge_labels)
        self.edge_directions = edge_directions
        self.edge_directions_reversed = edge_directions_reversed
        self.node_predicates = node_predicates
        self.edge_predicates = edge_predicates
        self.node_disjunction_index = node_disjunction_index
        self.node_conjunction_index = node_conjunction_index
        self.edge_disjunction_index = edge_disjunction_index
        self.edge_conjunction_index = edge_conjunction_index
        self.node_count = node_count

    def __inc__(self, key, value, *args, **kwargs):
        if key == "node_disjunction_index":
            return self.node_conjunction_index.size()[0]
        elif key == "node_conjunction_index":
            return self.num_nodes
        elif key == "edge_disjunction_index":
            return self.edge_conjunction_index.size()[0]
        elif key == "edge_conjunction_index":
            return self.num_edges
        else:
            return super().__inc__(key, value)
