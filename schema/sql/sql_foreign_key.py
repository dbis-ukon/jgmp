from typing import List

from schema.attribute import Attribute
from schema.schema_edge import SchemaEdge


class SQLForeignKey(SchemaEdge):
    def __init__(self, name: str, foreign_key_attributes: List[Attribute], primary_key_attributes: List[Attribute]) -> None:
        SchemaEdge.__init__(self, name)
        assert(len(foreign_key_attributes) == len(primary_key_attributes))
        self._foreign_key_attributes = foreign_key_attributes
        self._primary_key_attributes = primary_key_attributes

    def foreign_key_attributes(self) -> List[Attribute]:
        return self._foreign_key_attributes

    def primary_key_attributes(self) -> List[Attribute]:
        return self._primary_key_attributes
