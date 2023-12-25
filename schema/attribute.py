
from __future__ import annotations
from typing import Optional
from schema.data_type import DataType


class Attribute:
    def __init__(self, name: str, data_type: DataType, has_index: bool, nullable: bool = True, null_ratio: Optional[float] = None) -> None:
        self._name = name
        self._data_type = data_type
        self._has_index = has_index
        self._nullable = nullable
        self._null_ratio = null_ratio

    def name(self) -> str:
        return self._name

    def data_type(self) -> DataType:
        return self._data_type

    def has_index(self) -> bool:
        return self._has_index

    def nullable(self) -> bool:
        return self._nullable

    def null_ratio(self) -> Optional[float]:
        return self._null_ratio

    @staticmethod
    def convert_value(value):
        return value


class NumericAttribute(Attribute):
    def __init__(self, name: str, data_type: DataType, has_index: bool, minimum, maximum, nullable: bool = True, null_ratio: Optional[float] = None) -> None:
        Attribute.__init__(self, name, data_type, has_index, nullable=nullable, null_ratio=null_ratio)
        self._minimum = Attribute.convert_value(minimum)
        self._maximum = Attribute.convert_value(maximum)

    def minimum(self):
        return self._minimum

    def maximum(self):
        return self._maximum
