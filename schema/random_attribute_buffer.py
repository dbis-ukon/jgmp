
import abc
from typing import Any, Dict, List, Tuple
from schema.attributable import Attributable
from schema.attribute import Attribute
from schema.value_picker import ValuePicker


class RandomAttributeBuffer(ValuePicker):
    def __init__(self, buffer_size: int, fits: Dict[Tuple[Attributable, Attribute], bool]) -> None:
        assert(buffer_size > 0)
        self._buffer_size = buffer_size
        self._buffer: Dict[Tuple[Attributable, Attribute], List[Any]] = {}
        self._fits = fits
        for entity, attribute in fits:
            self._buffer[(entity, attribute)] = []

    def pick_random(self, entity: Attributable, attribute: Attribute) -> Any:
        if len(self._buffer[(entity, attribute)]) == 0:
            self._fill_buffer(entity, attribute)
        output = self._buffer[(entity, attribute)].pop()
        if self._fits[(entity, attribute)]:
            self._buffer[(entity, attribute)].append(output)
        return output

    @abc.abstractmethod
    def _fill_buffer(self, entity: Attributable, attribute: Attribute):
        pass
