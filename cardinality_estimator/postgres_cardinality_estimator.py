

from query.sql.sql_query import SQLQuery
from schema.sql.sql_schema import SQLSchema
from cardinality_estimator.cardinality_estimator import CardinalityEstimator


class PostgresCardinalityEstimator(CardinalityEstimator):
    def __init__(self, remember: bool, schema: SQLSchema) -> None:
        super().__init__(remember)
        self._schema = schema

    def _estimate(self, query: SQLQuery) -> float:
        connection = self._schema.connection()
        cursor = connection.cursor()
        cursor.execute("EXPLAIN (FORMAT JSON) " + query.sql())
        cardinality = cursor.fetchone()[0][0]["Plan"]["Plan Rows"]
        cursor.close()
        return cardinality
