
from __future__ import annotations
from schema.attribute import Attribute
from schema.sql.sql_foreign_key import SQLForeignKey
from schema.sql.sql_table import SQLTable
from typing import Dict, List, Optional, Set, Tuple, Union
from schema.graphlike_schema import GraphlikeSchema
import psycopg2


class SQLSchema(GraphlikeSchema):
    def __init__(self,
                 connection: psycopg2.connection,
                 nodes: List[SQLTable],
                 edges: List[Tuple[SQLTable, SQLForeignKey, SQLTable]],
                 name: str) -> None:
        GraphlikeSchema.__init__(self, [node for node in nodes], [edge for edge in edges])
        self._connection = connection
        self._name = name
        self._possible_equivalence_classes: Dict[Attribute, Set[Attribute]] = {}
        self._equivalence_connections: Dict[Tuple[Attribute, Attribute], List[Union[Tuple[SQLForeignKey, bool], SQLTable]]] = {}
        for node in nodes:
            for attribute in node.attributes():
                self._possible_equivalence_classes[attribute] = {attribute}
        for from_table, foreign_key, to_table in edges:
            for fk_attribute, pk_attribute in zip(foreign_key.foreign_key_attributes(), foreign_key.primary_key_attributes()):
                for fk_equ in self._possible_equivalence_classes[fk_attribute]:
                    if fk_equ == fk_attribute:
                        fk_sequence = []
                    else:
                        fk_sequence = self._equivalence_connections[(fk_equ, fk_attribute)] + [from_table]
                    for pk_equ in self._possible_equivalence_classes[pk_attribute]:
                        if pk_equ == pk_attribute:
                            pk_sequence = []
                        else:
                            pk_sequence = [to_table] + self._equivalence_connections[(pk_attribute, pk_equ)]
                        self._equivalence_connections[(fk_equ, pk_equ)] = fk_sequence + [(foreign_key, True)] + pk_sequence
                        self._equivalence_connections[(pk_equ, fk_equ)] = []
                        for elem in reversed(self._equivalence_connections[(fk_equ, pk_equ)]):
                            if isinstance(elem, SQLTable):
                                self._equivalence_connections[(pk_equ, fk_equ)].append(elem)
                            else:
                                assert(isinstance(elem, Tuple))
                                self._equivalence_connections[(pk_equ, fk_equ)].append((elem[0], not elem[1]))
                new_equivalence = self._possible_equivalence_classes[fk_attribute].union(self._possible_equivalence_classes[pk_attribute])
                for attribute in new_equivalence:
                    self._possible_equivalence_classes[attribute] = new_equivalence
        self._index_to_table: Dict[str, SQLTable] = {}
        for table in nodes:
            for index in table.indexes():
                self._index_to_table[index] = table


    def connection(self, timeout: Optional[int] = None):
        if timeout is None:
            return self._connection
        else:
            timeout_ms = timeout * 1000
            return psycopg2.connect(host="localhost", database=self._name, user="postgres", password="postgres", options='-c statement_timeout=%d' % timeout_ms)

    def foreign_key(self, fk_table: SQLTable, fk_column: Attribute, pk_table: SQLTable, pk_column: Attribute) -> Optional[SQLForeignKey]:
        for candidate, candidate_to in self._edges_from[fk_table]:
            assert(isinstance(candidate, SQLForeignKey))
            fk_attributes = candidate.foreign_key_attributes()
            pk_attributes = candidate.primary_key_attributes()
            if len(fk_attributes) == 1 and len(pk_attributes) == 1 and candidate_to == pk_table and fk_column == fk_attributes[0] and pk_column == pk_attributes[0]:
                return candidate
        return None

    def foreign_keys_from_column(self, fk_table: SQLTable, fk_column: Attribute) -> List[Tuple[SQLForeignKey, SQLTable]]:
        foreign_keys = []
        for candidate, candidate_to in self._edges_from[fk_table]:
            assert(isinstance(candidate, SQLForeignKey))
            if fk_column in candidate.foreign_key_attributes():
                foreign_keys.append((candidate, candidate_to))
        return foreign_keys

    def possible_equivalence_class(self, attribute: Attribute) -> Set[Attribute]:
        return self._possible_equivalence_classes[attribute]

    def equivalence_connection(self, from_attribute: Attribute, to_attribute: Attribute) -> List[Union[Tuple[SQLForeignKey, bool], SQLTable]]:
        return self._equivalence_connections[(from_attribute, to_attribute)]

    def index_to_table(self, index: str) -> SQLTable:
        return self._index_to_table[index]

    @staticmethod
    def sql_schema_from_connection(name: str, schema: str = "public", leafs: List[str] = [], mask: Optional[Set[str]] = None, fk_name: Optional[str] = None, port: Optional[int] = None) -> SQLSchema:
        if port is None:
            connection = psycopg2.connect(host="localhost", database=name, user="postgres", password="postgres")
        else:
            connection = psycopg2.connect(host="localhost", database=name, user="postgres", password="postgres", port=port)
        if fk_name is None:
            fk_connection = connection
        elif port is None:
            fk_connection = psycopg2.connect(host="localhost", database=fk_name, user="postgres", password="postgres")
        else:
            fk_connection = psycopg2.connect(host="localhost", database=fk_name, user="postgres", password="postgres", port=port)

        tables = SQLSchema._get_tables(connection, schema, leafs, mask)
        foreign_keys = SQLSchema._get_foreign_keys(fk_connection, schema, tables)

        return SQLSchema(connection, tables, foreign_keys, name)

    @staticmethod
    def _get_tables(connection: psycopg2.connection, schema: str, leafs: List[str], mask: Optional[Set[str]] = None) -> List[SQLTable]:
        tables = []

        cursor = connection.cursor()
        table_query = """SELECT table_name
                         FROM information_schema.tables
                         WHERE table_schema = '%s' AND table_name != 'plan_cache';""" % schema
        cursor.execute(table_query)
        for result in cursor.fetchall():
            table_name = result[0]
            if mask is None or table_name in mask:
                tables.append(SQLTable.table_from_connection(connection, schema, table_name, table_name in leafs))
        cursor.close()

        return tables

    @staticmethod
    def _get_foreign_keys(connection: psycopg2.connection, schema: str, tables: List[SQLTable]) -> List[Tuple[SQLTable, SQLForeignKey, SQLTable]]:
        table_dict = {table.name(): table for table in tables}
        foreign_keys = []

        cursor = connection.cursor()
        foreign_key_query = """SELECT DISTINCT tc.constraint_name, kcu.table_name, kcu.column_name, ccu.table_name, ccu.column_name
                                FROM information_schema.table_constraints AS tc
                                JOIN information_schema.key_column_usage AS kcu ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
                                JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
                                JOIN information_schema.key_column_usage AS kcup ON kcup.table_schema = tc.table_schema AND kcup.column_name = ccu.column_name AND kcup.ordinal_position = kcu.position_in_unique_constraint
                                AND ccu.table_schema = tc.table_schema
                                WHERE tc.constraint_schema = '%s'
                                AND tc.constraint_type = 'FOREIGN KEY';""" % schema
        cursor.execute(foreign_key_query)
        fk_references = {}
        for name, ft, fc, tt, tc in cursor.fetchall():
            if ft not in table_dict or tt not in table_dict:
                continue
            from_table = table_dict[ft]
            from_column = from_table.attribute(fc)
            to_table = table_dict[tt]
            to_column = to_table.attribute(tc)
            if name not in fk_references:
                fk_references[name] = (from_table, [], to_table, [])
            fk_references[name][1].append(from_column)
            fk_references[name][3].append(to_column)

        for name, (from_table, from_columns, to_table, to_columns) in fk_references.items():
            foreign_key = SQLForeignKey(name, from_columns, to_columns)
            foreign_keys.append((from_table, foreign_key, to_table))

        cursor.close()

        return foreign_keys
