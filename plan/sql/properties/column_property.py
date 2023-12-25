
from __future__ import annotations
from plan.property import Property
from query.sql.sql_table_instance import SQLTableInstance
from schema.attribute import Attribute


class ColumnProperty(Property):
    def __init__(self, name: str, table: SQLTableInstance, attribute: Attribute) -> None:
        super().__init__(name)
        self._table = table
        self._attribute = attribute

    def table(self) -> SQLTableInstance:
        return self._table

    def attribute(self) -> Attribute:
        return self._attribute

    def same_column(self, other: ColumnProperty) -> bool:
        return other.table() == self._table and other.attribute() == self._attribute

    def __hash__(self) -> int:
        return hash((self._name, self._table, self._attribute))

    def __eq__(self, other):
        if not isinstance(other, ColumnProperty):
            return False

        return type(self) == type(other) and self._table == other._table and self._attribute == other._attribute
