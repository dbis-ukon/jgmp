
import abc
from typing import Tuple, List, Optional
from query.graphlike_query import GraphlikeQuery
import time
from query.symmetry.eliminate_cross_product import detect_cross_product, eliminate_cross_product


class CardinalityEstimator:
    def __init__(self, remember: bool) -> None:
        self._remember = remember
        self._memory = {}

    def reset(self):
        self._memory = {}

    def estimate(self, query: Optional[GraphlikeQuery]) -> float:
        if query is None:
            return -1
        if self._remember:
            if query not in self._memory:
                self._memory[query] = self._estimate(query)
            return self._memory[query]
        if detect_cross_product(query):
            product = 1
            for factor_query in eliminate_cross_product(query):
                factor_estimate = self._estimate(factor_query)
                if factor_estimate == -1:
                    return -1
                product *= factor_estimate
            return product
        return self._estimate(query)

    def bulk_estimate(self, queries: List[List[Optional[GraphlikeQuery]]]) -> List[float]:
        estimates = []
        for query in queries:
            for subquery in query:
                estimates.append(self.estimate(subquery))
        return estimates

    def estimate_with_latency(self, query: GraphlikeQuery) -> Tuple[float, float]:
        start = time.time()
        estimate = self._estimate(query)
        latency = time.time() - start
        return estimate, latency

    @abc.abstractmethod
    def _estimate(self, query: GraphlikeQuery) -> float:
        pass

    def supports(self, query: GraphlikeQuery) -> bool:
        return True
