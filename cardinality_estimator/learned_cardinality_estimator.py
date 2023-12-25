import numpy as np
import torch
from typing import List, Optional
from torch_geometric.loader import DataLoader

from models.cardinality_model import CardinalityModel
from query.graphlike_query import GraphlikeQuery
from cardinality_estimator.cardinality_estimator import CardinalityEstimator
from query.symmetry.eliminate_cross_product import detect_cross_product, eliminate_cross_product
from train_cardinality import encode


class LearnedCardinalityEstimator(CardinalityEstimator):
    def __init__(self, cardinality_model: CardinalityModel, device: Optional[torch.device] = None) -> None:
        super().__init__(False)
        self._cardinality_model = cardinality_model
        self._device = device

    def cardinality_model(self) -> CardinalityModel:
        return self._cardinality_model

    def encoder(self):
        return self._cardinality_model.encoder()

    def bulk_estimate(self, queries: List[List[Optional[GraphlikeQuery]]]) -> List[float]:
        factor_queries = []
        factor_ids = []
        subquery_count = 0
        none_subqueries = []
        for query in queries:
            sub_factor_queries = []
            for subquery in query:
                if subquery is None or not self.supports(subquery):
                    none_subqueries.append(subquery_count)
                elif detect_cross_product(subquery):
                    for factor_query in eliminate_cross_product(subquery):
                        sub_factor_queries.append(factor_query)
                        factor_ids.append(subquery_count)
                else:
                    sub_factor_queries.append(subquery)
                    factor_ids.append(subquery_count)
                subquery_count += 1
            factor_queries.append(sub_factor_queries)

        encoded_queries = encode(factor_queries, self._cardinality_model, self._device)
        loader = DataLoader(encoded_queries, batch_size=1024)
        raw_estimations = []
        self._cardinality_model.eval()
        with torch.no_grad():
            for batch in loader:
                estimation = self._cardinality_model.forward(batch).cpu().numpy()
                raw_estimations += list(estimation)
        self._cardinality_model.train()
        estimations = np.ones(subquery_count)
        for fid, estimation in zip(factor_ids, raw_estimations):
            estimations[fid] *= estimation
        for none_subquery in none_subqueries:
            assert(estimations[none_subquery] == 1)
            estimations[none_subquery] = -1
        return list(estimations)

    def _estimate(self, query: Optional[GraphlikeQuery]) -> float:
        return self.bulk_estimate([[query]])[0]

    def save(self, path):
        torch.save(self._cardinality_model.state_dict(), path + ".pt")
        sampler = self._cardinality_model.encoder().sampler()
        if sampler is not None:
            sampler.save(path + ".samples")

    def supports(self, query: GraphlikeQuery) -> bool:
        return self._cardinality_model.supports(query)

