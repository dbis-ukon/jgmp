

from __future__ import annotations

import hashlib
from numbers import Number
from data.wrap_value import wrap_value
from schema.data_type import DATATYPES
from schema.attributable import Attributable
from typing import Any, List, Tuple
from schema.comparison_operator import ComparisonOperator
from schema.attribute import Attribute
from schema.comparison_operator import OPERATORS
import datetime


class Predicate:
    def __init__(self,
                 attribute: Attribute,
                 operator: ComparisonOperator,
                 value: Any,
                 positive: bool = True) -> None:
        assert(operator in attribute.data_type().operators())
        self._attribute = attribute
        self._operator = operator
        self._value = value
        self._positive = positive

    def attribute(self) -> Attribute:
        return self._attribute

    def operator(self) -> ComparisonOperator:
        return self._operator

    def value(self) -> Any:
        return self._value

    def positive(self) -> bool:
        return self._positive

    def query_string(self, predicatable_id: str):
        if self._attribute.data_type().python_type() == str and isinstance(self._value, Number):
            # check if string is numeric and convert
            if self._positive:
                positive_string = ""
            else:
                positive_string = "NOT "
            string = "(%s.%s ~ '^(?:[1-9]\\d*|0)?(?:\\.\\d+)?$' AND %s%s.%s::float %s %s)" % (predicatable_id,
                                                                                              self._attribute.name(),
                                                                                              positive_string,
                                                                                              predicatable_id,
                                                                                              self._attribute.name(),
                                                                                              self._operator.symbol(),
                                                                                              wrap_value(self._value))
        else:
            string = "%s.%s %s %s" % (predicatable_id, self._attribute.name(), self._operator.symbol(), wrap_value(self._value))
            if not self._positive:
                string = "NOT " + string
        return string

    def complement(self) -> Predicate:
        return Predicate(self._attribute, self._operator, self._value, positive=not self._positive)

    @staticmethod
    def from_json(json: Tuple[str, str, Any], labels: List[Attributable]):
        attribute_name, operator_symbol, value = json

        attribute = None
        for label in labels:
            if label.attribute_exists(attribute_name):
                attribute = label.attribute(attribute_name)
                break
        assert(attribute is not None)
        operator = OPERATORS[operator_symbol]
        if attribute.data_type() == DATATYPES["Date"]:
            value = datetime.datetime.strptime(value, "%Y-%m-%d")
        elif attribute.data_type() == DATATYPES["DateTime"]:
            value = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f000+00:00")
        return Predicate(attribute, operator, value)

    def __hash__(self):
        if isinstance(self._value, str) or isinstance(self._value, datetime.datetime) or isinstance(self._value, datetime.date):
            value_hash = int(hashlib.md5(str(self._value).encode("utf-8")).hexdigest(), 16)
        else:
            value_hash = self._value
        return hash((int(hashlib.md5(self._attribute.name().encode("utf-8")).hexdigest(), 16), int(hashlib.md5(self._operator.symbol().encode("utf-8")).hexdigest(), 16), value_hash, self._positive))


class ArbitraryPredicate:
    def __init__(self, predicate_string: str, positive: bool = True) -> None:
        self._predicate_string = predicate_string
        self._positive = positive

    def query_string(self, predicatable_id: str):
        if self._positive:
            return self._predicate_string
        else:
            return "NOT (" + self._predicate_string + ")"

    def complement(self) -> ArbitraryPredicate:
        return ArbitraryPredicate(self._predicate_string, positive=not self._positive)

    def __hash__(self):
        return hash((int(hashlib.md5(str(self._predicate_string).encode("utf-8")).hexdigest(), 16), self._positive))
