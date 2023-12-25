

from typing import Optional

import psycopg2
from query.sql.sql_query import SQLQuery
from schema.sql.sql_schema import SQLSchema
from cardinality_estimator.cardinality_estimator import CardinalityEstimator


class PostgresTrueCardinality(CardinalityEstimator):
    def __init__(self, remember: bool, schema: SQLSchema, timeout: Optional[int] = 10) -> None:
        super().__init__(remember)
        self._schema = schema
        self._timeout = timeout

    def _estimate(self, query: SQLQuery) -> float:
        connection = self._schema.connection(timeout=self._timeout)
        cursor = connection.cursor()
        try:
            cursor.execute(query.sql(count=True))
        except psycopg2.extensions.QueryCanceledError:
            return -1
        except psycopg2.InternalError:
            return -1
        cardinality = cursor.fetchone()[0]
        cursor.close()
        return cardinality
