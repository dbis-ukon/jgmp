
from __future__ import annotations

from typing import List, Tuple, Set
from schema.attributable import Attributable
from schema.schema_node import SchemaNode

import psycopg2

from schema.sql.sql_column import SQLColumn


class SQLTable(SchemaNode, Attributable):
    def __init__(self, name: str, cardinality: int, attributes: List[SQLColumn], key_columns: List[SQLColumn], oid: int, is_leaf: bool, indexes: List[str] = []) -> None:
        SchemaNode.__init__(self, name, cardinality, is_leaf=is_leaf)
        Attributable.__init__(self, attributes)
        self._key_columns = key_columns
        self._oid = oid
        self._indexes = set(indexes)

    def is_key(self, column: SQLColumn) -> bool:
        return column in self._key_columns

    def key_columns(self) -> List[SQLColumn]:
        return self._key_columns

    def oid(self) -> int:
        return self._oid

    def indexes(self) -> Set[str]:
        return self._indexes

    @staticmethod
    def table_from_connection(connection: psycopg2.connection, schema: str, name: str, is_leaf: bool) -> SQLTable:
        cardinality = SQLTable._get_cardinality(connection, name)
        columns, key_columns = SQLTable._get_columns(connection, schema, name, cardinality)
        oid = SQLTable._get_oid(connection, name)
        indexes = SQLTable._get_indexes(connection, schema, name)
        return SQLTable(name, cardinality, columns, key_columns, oid, is_leaf, indexes)

    @staticmethod
    def _get_cardinality(connection: psycopg2.connection, name: str) -> int:
        cursor = connection.cursor()
        cardinality_query = "SELECT COUNT(*) FROM %s;" % name
        cursor.execute(cardinality_query)
        cardinality = cursor.fetchone()[0]
        cursor.close()
        return cardinality

    @staticmethod
    def _get_columns(connection: psycopg2.connection, schema: str, name: str, cardinality: int) -> Tuple[List[SQLColumn], List[SQLColumn]]:
        columns = []
        key_columns = []
        cursor = connection.cursor()
        column_query = """  SELECT c.column_name, kcu.ordinal_position IS NOT NULL
                            FROM information_schema.columns AS c
                            LEFT OUTER JOIN information_schema.key_column_usage AS kcu ON kcu.table_name = c.table_name
                                                                                       AND kcu.column_name = c.column_name
                                                                                       AND kcu.position_in_unique_constraint IS NULL
                            WHERE c.table_schema = '%s'
                                AND c.table_name = '%s';""" % (schema, name)
        cursor.execute(column_query)
        for column_name, is_key in cursor.fetchall():
            attribute = SQLColumn.build_sql_column(connection, schema, name, column_name, cardinality)
            columns.append(attribute)
            if is_key:
                key_columns.append(attribute)
        cursor.close()
        return columns, key_columns

    @staticmethod
    def _get_oid(connection: psycopg2.connection, name: str) -> int:
        cursor = connection.cursor()
        oid_query = "SELECT oid FROM pg_class WHERE relkind = 'r' AND relname = '%s';" % name
        cursor.execute(oid_query)
        oid = cursor.fetchone()[0]
        cursor.close()
        return oid

    @staticmethod
    def _get_indexes(connection: psycopg2.connection, schema: str, name: str) -> List[str]:
        cursor = connection.cursor()
        index_query = "SELECT indexname FROM pg_indexes WHERE tablename = '%s' AND schemaname = '%s';" % (name, schema)
        cursor.execute(index_query)
        indexes = [index[0] for index in cursor.fetchall()]
        cursor.close()
        return indexes
