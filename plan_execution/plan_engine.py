from abc import abstractmethod
from typing import Dict, FrozenSet, Optional, Tuple

from plan.expression import Expression
from query.graphlike_query import GraphlikeQuery
from query.query_node import QueryNode
from schema.graphlike_schema import GraphlikeSchema


class PlanEngine:
    def __init__(self, schema: GraphlikeSchema, is_stubborn: bool, timeout: Optional[int] = None) -> None:
        self._schema = schema
        self._timeout = timeout
        self._is_stubborn = is_stubborn

    def timeout(self) -> Optional[int]:
        return self._timeout

    def is_stubborn(self) -> bool:
        return self._is_stubborn

    @abstractmethod
    def execute(self, query: GraphlikeQuery, plan: Optional[Expression]) -> Optional[Tuple[bool, float, Dict[FrozenSet[QueryNode], int], bool]]:
        pass
