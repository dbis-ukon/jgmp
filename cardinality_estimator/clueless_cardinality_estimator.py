from cardinality_estimator.cardinality_estimator import CardinalityEstimator


class CluelessCardinalityEstimator(CardinalityEstimator):
    def __init__(self):
        super().__init__(False)

    def _estimate(self, query):
        return -1