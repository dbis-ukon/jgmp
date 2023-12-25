
from random import Random
from typing import List
from query.predicate import Predicate
from schema.value_picker import ValuePicker
from schema.attributable import Attributable
from schema.attribute import Attribute
from schema.comparison_operator import OPERATORS, ComparisonOperator
from schema.data_type import EncodingType


def simple_predicate(operator: ComparisonOperator,
                     value_picker: ValuePicker,
                     entity: Attributable,
                     attribute: Attribute,
                     positive: bool = True) -> List[List[Predicate]]:
    if attribute.null_ratio() == 1:
        return []

    value = value_picker.pick_random(entity, attribute)
    return [[Predicate(attribute, operator, value, positive=positive)]]


def equal_disjunction_predicate(value_picker: ValuePicker,
                                random: Random,
                                entity: Attributable,
                                attribute: Attribute) -> List[List[Predicate]]:
    if attribute.null_ratio() == 1:
        return []

    disjunction_length = random.randint(2, 5)
    disjunction_values = set()
    for _ in range(disjunction_length):
        value = None
        i = 0
        while (value is None or value in disjunction_values) and i < 10:
            value = value_picker.pick_random(entity, attribute)
            i += 1
        disjunction_values.add(value)
    return [[Predicate(attribute, OPERATORS["="], value) for value in disjunction_values]]


def equal_predicate(value_picker: ValuePicker,
                    random: Random,
                    entity: Attributable,
                    attribute: Attribute) -> List[List[Predicate]]:
    positive = random.random() < 0.9
    return simple_predicate(OPERATORS["="], value_picker, entity, attribute, positive=positive)


def less_predicate(value_picker: ValuePicker,
                   random: Random,
                   entity: Attributable,
                   attribute: Attribute) -> List[List[Predicate]]:
    return simple_predicate(OPERATORS["<"], value_picker, entity, attribute)


def greater_predicate(value_picker: ValuePicker,
                      random: Random,
                      entity: Attributable,
                      attribute: Attribute) -> List[List[Predicate]]:
    return simple_predicate(OPERATORS[">"], value_picker, entity, attribute)


def between_predicate(value_picker: ValuePicker,
                      random: Random,
                      entity: Attributable,
                      attribute: Attribute) -> List[List[Predicate]]:
    if attribute.null_ratio() == 1:
        return []

    values = set()
    for _ in range(2):
        value = value_picker.pick_random(entity, attribute)
        values.add(value)
    return [[Predicate(attribute, OPERATORS["<"], min(values), positive=False)], [Predicate(attribute, OPERATORS[">"], max(values), positive=False)]]


def like_value(value_picker: ValuePicker,
               random: Random,
               entity: Attributable,
               attribute: Attribute,
               beginning: bool) -> str:
    whole = value_picker.pick_random(entity, attribute)
    assert(isinstance(whole, str))
    parts = whole.split()
    if beginning:
        part = parts[0]
        part = part + "%"
    else:
        part = random.choice(parts)
        part = "%" + part + "%"
    return part


def like_predicate(value_picker: ValuePicker,
                   random: Random,
                   entity: Attributable,
                   attribute: Attribute) -> List[List[Predicate]]:
    if attribute.null_ratio() == 1:
        return []

    beginning = random.random() < 0.2
    multi = random.random() < 0.1
    positive = multi or random.random() < 0.9
    if multi:
        disjunction_length = random.randint(2, 5)
    else:
        disjunction_length = 1

    values = set()
    for _ in range(disjunction_length):
        part = like_value(value_picker, random, entity, attribute, beginning)
        values.add(part)

    return [[Predicate(attribute, OPERATORS["LIKE"], value, positive=positive) for value in values]]


def null_predicate(value_picker: ValuePicker,
                   random: Random,
                   entity: Attributable,
                   attribute: Attribute) -> List[List[Predicate]]:
    if attribute.null_ratio() == 0 or attribute.null_ratio() == 1:
        return []
    return [[Predicate(attribute, OPERATORS["IS"], None, positive=random.random() < 0.5)]]


_numeric_predicates = {equal_predicate: 0.25, less_predicate: 0.25, greater_predicate: 0.25, between_predicate: 0.25}
_string_predicates = {equal_predicate: 0.4, equal_disjunction_predicate: 0.1, like_predicate: 0.45, null_predicate: 0.05}

PREDICATE_GENERATORS = {EncodingType.NUMERIC: _numeric_predicates, EncodingType.STRING: _string_predicates}
