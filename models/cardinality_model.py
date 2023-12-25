from abc import abstractmethod
from typing import Union, List

import torch
from torch import FloatTensor
from torch.nn import Module

from encoder.encoder import Encoder
from models.util import aggregate_many_to_many
from query.graphlike_query import GraphlikeQuery
from query_data.bulk_relation_query_data import BulkRelationQueryData
from query_data.cardinality_relation_query_data import CardinalityRelationQueryData


class CardinalityModel(Module):
    def __init__(self, encoder: Encoder, loss: Module):
        super(CardinalityModel, self).__init__()
        self._encoder = encoder
        self._loss = loss

    def encoder(self) -> Encoder:
        return self._encoder

    @abstractmethod
    def encode(self, query: GraphlikeQuery, cardinality: int):
        pass

    def bulk_encode(self, queries: List[GraphlikeQuery]):
        encodings = []
        for query in queries:
            cardinalities = query.cardinality_estimates()
            if "true" in cardinalities:
                cardinality = cardinalities["true"]
            else:
                cardinality = -1
            encodings.append(self.encode(query, cardinality))
        return encodings

    def loss(self, data) -> FloatTensor:
        out = self.forward(data)
        labels = data.cardinality
        return self._loss(out, labels)

    def loss_relation(self, data: Union[CardinalityRelationQueryData, BulkRelationQueryData]) -> FloatTensor:
        out = self.forward(data)

        left_out = aggregate_many_to_many(out, data.left, data.equal.size()[0])
        right_out = aggregate_many_to_many(out, data.right, data.equal.size()[0])

        left_log = torch.log(left_out)
        right_log = torch.log(right_out)

        diff = left_log - right_log

        equal_case = diff
        greater_case = torch.relu(-diff)
        conditional = torch.where(data.equal, equal_case, greater_case)

        return torch.square(conditional)

    @staticmethod
    def supports(query: GraphlikeQuery) -> bool:
        return True
