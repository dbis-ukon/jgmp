import abc
from typing import Any
from schema.attributable import Attributable
from schema.attribute import Attribute


class ValuePicker:
    @abc.abstractmethod
    def pick_random(self, entity: Attributable, attribute: Attribute) -> Any:
        pass
