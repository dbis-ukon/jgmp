

from enum import Enum
from typing import List
from query.graphlike_query import GraphlikeQuery


class RelationType(Enum):
    EQUAL = 1
    GREATEREQUAL = 2


class CardinalityRelation:
    def __init__(self, left: List[GraphlikeQuery], right: List[GraphlikeQuery], type: RelationType) -> None:
        self._left = left
        self._right = right
        self._type = type

    def left(self) -> List[GraphlikeQuery]:
        return self._left

    def right(self) -> List[GraphlikeQuery]:
        return self._right

    def type(self) -> RelationType:
        return self._type
