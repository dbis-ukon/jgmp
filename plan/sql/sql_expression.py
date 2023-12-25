
from __future__ import annotations
from query.sql.sql_query import SQLQuery
from typing import List
from plan.operator import Operator
from plan.expression import Expression


class SQLExpression(Expression):
    def __init__(self, operator: Operator, children: List[SQLExpression]) -> None:
        super().__init__(operator, children)

    def subquery(self) -> SQLQuery:
        nodes, edges = self._expression_represents()
        return SQLQuery(nodes, edges)
