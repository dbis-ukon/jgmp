

from plan.property import Property
from plan.sql.properties.column_property import ColumnProperty
from query.sql.sql_table_instance import SQLTableInstance
from schema.attribute import Attribute


class SortedProperty(ColumnProperty):
    def __init__(self, table: SQLTableInstance, attribute: Attribute) -> None:
        super().__init__("Sorted", table, attribute)

    def includes(self, other: Property) -> bool:
        if isinstance(other, SortedProperty):
            return self.same_column(other)
        return False
