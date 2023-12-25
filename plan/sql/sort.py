

from typing import Any, Dict, FrozenSet, List, Optional, Set
from plan.operator import Operator
from plan.property import Property
from plan.sql.properties.sorted_property import SortedProperty
from query.graphlike_query import GraphlikeQuery
from query.sql.sql_table_instance import SQLTableInstance
from schema.attribute import Attribute


class Sort(Operator):
    def __init__(self, table: SQLTableInstance, attribute: Attribute) -> None:
        super().__init__("sort", 1, [], [])
        self._table = table
        self._attribute = attribute

    def table(self) -> SQLTableInstance:
        return self._table

    def attribute(self) -> Attribute:
        return self._attribute

    def string(self, alias: Dict[Any, str] = {}) -> str:
        if self._table in alias:
            table_name = alias[self._table]
        else:
            table_name = self._table.table().name()
        return "%s[%s.%s]" % (self._name, table_name, self._attribute.name())

    def requires(self,
                 required: Set[Property],
                 children: List[GraphlikeQuery]) -> Optional[List[FrozenSet[Property]]]:
        sorted_property = SortedProperty(self._table, self._attribute)
        for requirement in required:
            if not sorted_property.includes(requirement):
                return None
        return [frozenset()]
