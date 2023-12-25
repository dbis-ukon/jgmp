

from typing import Any, Dict, FrozenSet, List, Optional
from plan.property import Property
from plan.sql.properties.index_property import IndexProperty
from plan.sql.scan.scan import Scan
from query.graphlike_query import GraphlikeQuery
from query.sql.sql_table_instance import SQLTableInstance
from schema.attribute import Attribute


class IndexScan(Scan):
    def __init__(self, table: SQLTableInstance, attribute: Attribute) -> None:
        super().__init__(table, name="indexScan")
        self._attribute = attribute

    def attribute(self) -> Attribute:
        return self._attribute

    def string(self, alias: Dict[Any, str] = {}) -> str:
        if self._table in alias:
            table_name = alias[self._table]
        else:
            table_name = self._table.table().name()
        return "%s[%s](%s)" % (self._name, self._attribute.name(), table_name)

    def requires(self,
                 required: FrozenSet[Property],
                 children: List[GraphlikeQuery]) -> Optional[List[FrozenSet[Property]]]:
        if not self._attribute.has_index():
            return None
        index_property = IndexProperty(self._table, self._attribute)
        for requirement in required:
            if not index_property.includes(requirement):
                return None
        return []
