

from plan.property import Property
from plan.sql.properties.column_property import ColumnProperty
from plan.sql.properties.sorted_property import SortedProperty
from query.sql.sql_table_instance import SQLTableInstance
from schema.attribute import Attribute


class IndexProperty(ColumnProperty):
    def __init__(self, table: SQLTableInstance, attribute: Attribute) -> None:
        super().__init__("Index", table, attribute)

    def includes(self, other: Property) -> bool:
        if isinstance(other, IndexProperty) or isinstance(other, SortedProperty):
            return self.same_column(other)
        return False
