

from __future__ import annotations
from query.query_utility import predicates_to_string
from schema.sql.sql_schema import SQLSchema
from query.predicate import Predicate, ArbitraryPredicate
from typing import List, Union
from schema.sql.sql_table import SQLTable
from query.predicatable import Predicatable
from query.query_node import QueryNode


class SQLTableInstance(QueryNode, Predicatable):
    def __init__(self, table: SQLTable, cardinality: float, predicates: List[List[Union[Predicate, ArbitraryPredicate]]], virtual: bool = False) -> None:
        labels = [table]
        QueryNode.__init__(self, labels, cardinality, virtual=virtual)
        Predicatable.__init__(self, labels, predicates)

    def table(self) -> SQLTable:
        assert(len(self.labels()) == 1)
        table = self.labels()[0]
        assert(isinstance(table, SQLTable))
        return table

    @staticmethod
    def sql(table: SQLTable, predicates: List[List[Union[Predicate, ArbitraryPredicate]]], alias: str = "t") -> str:
        predicate_string = predicates_to_string(alias, predicates)
        if len(predicate_string) > 0:
            predicate_string = "\nWHERE " + predicate_string
        query = """SELECT *
                   FROM %s AS %s%s;""" % (table.name(), alias, predicate_string)
        return query

    @staticmethod
    def build(schema: SQLSchema, table: SQLTable, predicates: List[List[Predicate]], alias: str = "t") -> SQLTableInstance:
        if len(predicates) == 0:
            cardinality = table.cardinality()
        else:
            connection = schema.connection()
            cursor = connection.cursor()
            cardinality_query = "EXPLAIN (FORMAT JSON) " + SQLTableInstance.sql(table, predicates, alias=alias)
            cursor.execute(cardinality_query)
            cardinality = cursor.fetchone()[0][0]["Plan"]["Plan Rows"]
            cursor.close()

        return SQLTableInstance(table, cardinality, predicates)

    def copy(self) -> SQLTableInstance:
        return SQLTableInstance(self._labels[0], self._cardinality, self._predicates, virtual=self._virtual)

    def hash(self):
        return hash(frozenset([frozenset(p) for p in self._predicates]))
