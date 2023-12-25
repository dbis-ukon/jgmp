
from schema.attribute import Attribute
from typing import List


class Attributable:
    def __init__(self, attributes: List[Attribute]) -> None:
        self._attributes = attributes
        self._attribute_dict = {attribute.name(): attribute for attribute in attributes}
        self._attribute_numbers = {attribute: i + 1 for i, attribute in enumerate(attributes)}

    def attributes(self) -> List[Attribute]:
        return self._attributes

    def attribute(self, name: str) -> Attribute:
        return self._attribute_dict[name]

    def attribute_exists(self, name: str) -> bool:
        return name in self._attribute_dict

    def attribute_number(self, attribute: Attribute) -> int:
        return self._attribute_numbers[attribute]
