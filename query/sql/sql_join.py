
from __future__ import annotations
from typing import Dict, List, Tuple
from query.sql.sql_table_instance import SQLTableInstance

from schema.sql.sql_schema import SQLSchema
from schema.sql.sql_foreign_key import SQLForeignKey
from query.query_edge import EdgeDirection, QueryEdge


class SQLJoin(QueryEdge):
    def __init__(self, foreign_key: SQLForeignKey) -> None:
        QueryEdge.__init__(self, [foreign_key], EdgeDirection.NATURAL)

    def foreign_key(self) -> SQLForeignKey:
        return self._labels[0]

    def copy(self) -> SQLJoin:
        return SQLJoin(self._labels[0])
