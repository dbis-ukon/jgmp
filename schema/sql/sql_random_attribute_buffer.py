
from random import Random
from typing import Dict, Optional
from schema.random_attribute_buffer import RandomAttributeBuffer
from schema.sql.sql_column import SQLColumn
from schema.sql.sql_schema import SQLSchema
from schema.sql.sql_table import SQLTable


class SQLRandomAttributeBuffer(RandomAttributeBuffer):
    def __init__(self, schema: SQLSchema, buffer_size: int = 1000, seed: Optional[int] = None) -> None:
        RandomAttributeBuffer.__init__(self,
                                       buffer_size,
                                       {(node, attribute): node.cardinality() * (1 - attribute.null_ratio()) <= buffer_size for node in schema.nodes() for attribute in node.attributes()})
        self._random = Random(seed)
        self._connection = schema.connection()

        self._column_to_table: Dict[SQLColumn, SQLTable] = {}
        for table in schema.nodes():
            assert(isinstance(table, SQLTable))
            for column in table.attributes():
                self._column_to_table[column] = table
        self._ratios = {node: min(buffer_size / node.cardinality(), 1) for node in schema.nodes()}

    def _fill_buffer(self, entity: SQLTable, attribute: SQLColumn):
        cursor = self._connection.cursor()
        sample_query = """SELECT %s
                          FROM %s
                          TABLESAMPLE SYSTEM(%f) REPEATABLE (%d)
                          WHERE NOT %s IS NULL;""" % (attribute.name(),
                                                      entity.name(),
                                                      self._ratios[entity] / (1 - attribute.null_ratio()) * 100,
                                                      self._random.randint(0, 1000000),
                                                      attribute.name())
        cursor.execute(sample_query)
        self._buffer[(entity, attribute)] = []
        for row in cursor.fetchall():
            self._buffer[(entity, attribute)].append(row[0])
        cursor.close()
        if len(self._buffer[(entity, attribute)]) == 0:
            self._fill_buffer(entity, attribute)
