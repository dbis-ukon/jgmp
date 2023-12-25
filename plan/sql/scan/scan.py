
from typing import Any, Dict, FrozenSet, List, Optional
from plan.property import Property
from query.graphlike_query import GraphlikeQuery
from query.sql.sql_table_instance import SQLTableInstance
from plan.operator import Operator


class Scan(Operator):
    def __init__(self, table: SQLTableInstance, name: str = "scan") -> None:
        super().__init__(name, 0, [table], [])
        self._table = table

    def table(self) -> SQLTableInstance:
        return self._table

    def string(self, alias: Dict[Any, str] = {}) -> str:
        if self._table in alias:
            table_name = alias[self._table]
        else:
            table_name = self._table.table().name()
        return "%s(%s)" % (self._name, table_name)

    def requires(self,
                 required: FrozenSet[Property],
                 children: List[GraphlikeQuery]) -> Optional[List[FrozenSet[Property]]]:
        return []
