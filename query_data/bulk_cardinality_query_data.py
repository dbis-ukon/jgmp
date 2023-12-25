from torch import FloatTensor, LongTensor

from query_data.bulk_query_data import BulkQueryData


class BulkCardinalityQueryData(BulkQueryData):
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
                 cardinality: FloatTensor) -> None:
        super(BulkCardinalityQueryData, self).__init__(node_index,
                                                       edge_index,
                                                       edge_labels,
                                                       shared_node_labels,
                                                       shared_node_cardinalities,
                                                       shared_node_samples,
                                                       shared_node_predicates,
                                                       shared_node_disjunction_index,
                                                       shared_node_conjunction_index,
                                                       my_batch)
        self.cardinality = cardinality
