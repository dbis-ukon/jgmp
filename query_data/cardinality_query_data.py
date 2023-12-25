
from query_data.graphlike_query_data import GraphlikeQueryData
from torch import FloatTensor, LongTensor


class CardinalityQueryData(GraphlikeQueryData):
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
                 cardinality: FloatTensor) -> None:
        super(CardinalityQueryData, self).__init__(node_labels,
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
        self.cardinality = cardinality
