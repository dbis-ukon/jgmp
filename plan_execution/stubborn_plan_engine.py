import json
from typing import Dict, FrozenSet, Optional, Tuple, List, Union

import psycopg2
from plan.sql.sql_expression import SQLExpression
from plan_execution.pg_explain import PgExplain
from plan_execution.plan_engine import PlanEngine
from query.sql.sql_query import SQLQuery
from query.sql.sql_table_instance import SQLTableInstance
from schema.sql.sql_schema import SQLSchema


class StubbornPlanEngine(PlanEngine):
    def __init__(self,
                 schema: SQLSchema,
                 disable_gather: bool,
                 disable_hash_join: bool = False,
                 disable_merge_join: bool = False,
                 disable_nested_loop: bool = False,
                 count: bool = False,
                 timeout: Optional[int] = None) -> None:
        super().__init__(schema, True, timeout=timeout)
        self._pg_explain = PgExplain(True)
        self._disable_gather = disable_gather
        self._disable_hash_join = disable_hash_join
        self._disable_merge_join = disable_merge_join
        self._disable_nested_loop = disable_nested_loop
        self._count = count

    def execute(self, query: SQLQuery, plan: Optional[SQLExpression]) -> Optional[Tuple[bool, float, Dict[FrozenSet[SQLTableInstance], int], bool]]:
        return self.execute_with_settings(query, plan, [])

    def execute_with_settings(self,
                              query: Union[SQLQuery, str],
                              plan: Optional[SQLExpression],
                              settings: List[str],
                              print_explain: bool = False,
                              analyze: bool = True
                              ) -> Optional[Tuple[bool, float, Dict[FrozenSet[SQLTableInstance], int], bool]]:
        cursor = self._schema.connection(timeout=self._timeout).cursor()
        if self._disable_gather:
            cursor.execute("SET LOCAL max_parallel_workers_per_gather = 0;")

        if self._disable_hash_join:
            cursor.execute("SET LOCAL enable_hashjoin = OFF;")
        if self._disable_merge_join:
            cursor.execute("SET LOCAL enable_mergejoin = OFF;")
        if self._disable_nested_loop:
            cursor.execute("SET LOCAL enable_nestloop = OFF;")

        for setting in settings:
            cursor.execute(setting)

        if isinstance(query, str):
            sql = query
        else:
            sql = query.sql(count=self._count)
        plan_implementation = "EXPLAIN (ANALYZE %s, FORMAT JSON) %s" % (str(analyze).capitalize(), sql)

        try:
            cursor.execute(plan_implementation)
        except psycopg2.extensions.QueryCanceledError:
            return None

        explain = cursor.fetchone()[0][0]
        if print_explain:
            print(json.dumps([explain], indent=4))

        cursor.close()

        if not analyze:
            return None

        execution_time = self._pg_explain.execution_time(explain)
        if isinstance(query, str):
            cardinalities = {}
        else:
            cardinalities = self._pg_explain.cardinalities(query, explain)
        gather = self._pg_explain.contains_gather(explain)

        if self._disable_gather and gather:
            print("Gather used anyway")

        return True, execution_time, cardinalities, gather
