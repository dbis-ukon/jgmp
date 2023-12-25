from torch import FloatTensor, LongTensor

from query_data.bulk_mscn_query_data import BulkMSCNQueryData
from query_data.bulk_query_data import BulkQueryData


class BulkMSCNCardinalityQueryData(BulkMSCNQueryData):
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
                 my_batch: LongTensor,
                 cardinality: FloatTensor) -> None:
        super(BulkMSCNCardinalityQueryData, self).__init__(node_index,
                                                           edge_index,
                                                           edge_labels,
                                                           shared_node_labels,
                                                           shared_node_label_vectors,
                                                           shared_node_cardinalities,
                                                           shared_node_samples,
                                                           shared_node_predicates,
                                                           shared_node_predicate_index,
                                                           my_batch)
        self.cardinality = cardinality
