

from typing import FrozenSet, List, Optional
from plan.property import Property
from plan.sql.scan.scan import Scan
from query.graphlike_query import GraphlikeQuery
from query.sql.sql_table_instance import SQLTableInstance


class SequentialScan(Scan):
    def __init__(self, table: SQLTableInstance) -> None:
        super().__init__(table, name="seqScan")

    def requires(self,
                 required: FrozenSet[Property],
                 children: List[GraphlikeQuery]) -> Optional[List[FrozenSet[Property]]]:
        if len(required) == 0:
            return []
        return None
