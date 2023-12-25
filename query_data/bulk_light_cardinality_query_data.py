from torch import FloatTensor, LongTensor
from query_data.bulk_light_query_data import BulkLightQueryData


class BulkLightCardinalityQueryData(BulkLightQueryData):
    def __init__(self,
                 node_index: LongTensor,
                 edge_index: LongTensor,
                 edge_labels: FloatTensor,
                 shared_node_labels: LongTensor,
                 shared_node_cardinalities: FloatTensor,
                 shared_node_samples: FloatTensor,
                 my_batch: LongTensor,
                 cardinality: FloatTensor) -> None:
        super(BulkLightCardinalityQueryData, self).__init__(node_index,
                                                            edge_index,
                                                            edge_labels,
                                                            shared_node_labels,
                                                            shared_node_cardinalities,
                                                            shared_node_samples,
                                                            my_batch)
        self.cardinality = cardinality
