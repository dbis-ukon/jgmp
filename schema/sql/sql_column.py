from decimal import Decimal
from typing import Any, Optional
from schema.attribute import Attribute, NumericAttribute
from schema.data_type import DATATYPES, DataType, EncodingType


class SQLColumn(Attribute):
    def __init__(self,
                 name: str,
                 data_type: DataType,
                 has_index: bool,
                 index_id: Optional[int],
                 atttypid: int,
                 atttypmod: int,
                 nullable: bool,
                 null_ratio: float
                 ) -> None:
        Attribute.__init__(self, name, data_type, has_index, nullable=nullable, null_ratio=null_ratio)
        self._index_id = index_id
        self._atttypid = atttypid
        self._atttypmod = atttypmod

    def index_id(self) -> Optional[int]:
        return self._index_id

    def atttypid(self) -> int:
        return self._atttypid

    def atttypmod(self) -> int:
        return self._atttypmod

    @staticmethod
    def build_sql_column(connection, schema: str, table: str, column: str, cardinality: int) -> Attribute:
        cursor = connection.cursor()
        column_query = """  SELECT isc.data_type, a.atttypid, a.atttypmod, MIN(ix.indexrelid), isc.is_nullable = 'YES'
                            FROM information_schema.columns AS isc
                            JOIN pg_namespace AS n ON n.nspname = isc.table_schema
                            JOIN pg_class AS t ON t.relnamespace = n.oid AND t.relname = isc.table_name
                            JOIN pg_attribute AS a ON a.attrelid = t.oid AND a.attname = isc.column_name
                            LEFT JOIN pg_index AS ix ON ix.indrelid = t.oid AND a.attnum = ANY(ix.indkey) AND NOT a.attnum != SOME(ix.indkey)
                            WHERE t.relkind = 'r'
                                AND isc.table_schema = '%s'
                                AND isc.table_name = '%s'
                                AND isc.column_name = '%s'
                            GROUP BY isc.data_type, a.atttypid, a.atttypmod, isc.is_nullable;""" % (schema, table, column)
        cursor.execute(column_query)
        data_type_sql, atttypid, atttypmod, index_id, nullable = cursor.fetchone()
        has_index = index_id is not None
        data_type = DATATYPES[data_type_sql]

        if nullable:
            null_ratio_query = """SELECT COUNT(*)
                                  FROM %s
                                  WHERE %s IS NULL;""" % (table, column)
            cursor.execute(null_ratio_query)
            null_count = cursor.fetchone()[0]
            if cardinality == 0:
                null_ratio = 1
            else:
                null_ratio = null_count / cardinality
        else:
            null_ratio = 0

        cursor.close()

        if data_type.encoding_type() == EncodingType.NUMERIC:
            return NumericSQLColumn.build_sql_column(connection,
                                                     schema,
                                                     table,
                                                     column,
                                                     data_type,
                                                     has_index,
                                                     index_id,
                                                     atttypid,
                                                     atttypmod,
                                                     nullable,
                                                     null_ratio)

        return SQLColumn(column,
                         data_type,
                         has_index,
                         index_id,
                         atttypid,
                         atttypmod,
                         nullable,
                         null_ratio)


class NumericSQLColumn(SQLColumn, NumericAttribute):
    def __init__(self,
                 name: str,
                 data_type: DataType,
                 has_index: bool,
                 index_id: Optional[int],
                 atttypid: int,
                 atttypmod: int,
                 minimum: Any,
                 maximum: Any,
                 nullable: bool,
                 null_ratio: float
                 ) -> None:
        SQLColumn.__init__(self, name, data_type, has_index, index_id, atttypid, atttypmod, nullable, null_ratio)
        NumericAttribute.__init__(self, name, data_type, has_index, minimum, maximum, nullable, null_ratio)

    @staticmethod
    def build_sql_column(connection,
                         schema: str,
                         table: str,
                         column: str,
                         data_type: DataType,
                         has_index: bool,
                         index_id: Optional[int],
                         atttypid: int,
                         atttypmod: int,
                         nullable: bool,
                         null_ratio: float
                         ) -> NumericAttribute:
        cursor = connection.cursor()
        range_query = """   SELECT MIN(%s), MAX(%s)
                            FROM %s.%s;""" % (column, column, schema, table)
        cursor.execute(range_query)

        minimum, maximum = cursor.fetchone()

        if isinstance(minimum, Decimal):
            minimum = float(minimum)
        if isinstance(maximum, Decimal):
            maximum = float(maximum)

        cursor.close()

        return NumericSQLColumn(column,
                                data_type,
                                has_index,
                                index_id,
                                atttypid,
                                atttypmod,
                                minimum,
                                maximum,
                                nullable,
                                null_ratio)
