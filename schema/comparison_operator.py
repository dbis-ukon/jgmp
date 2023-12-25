
from typing import Any, Callable
import re


class ComparisonOperator:
    def __init__(self, symbol: str, comparison_function: Callable[[Any, Any], bool]) -> None:
        self._symbol = symbol
        self._comparison_function = comparison_function

    def symbol(self) -> str:
        return self._symbol

    def compare(self, obj_a: Any, obj_b: Any) -> bool:
        return self._comparison_function(obj_a, obj_b)


def eval_equal(a, b):
    if isinstance(a, str) and isinstance(b, str):
        if len(a) < len(b):
            return a + ' ' * (len(b) - len(a)) == b
        elif len(a) > len(b):
            return a == b + ' ' * (len(a) - len(b))
    return a is not None and a == b


def eval_lesser(a, b):
    if isinstance(a, str) and not isinstance(b, str):
        try:
            return float(a) < b
        except ValueError:
            return False
    return a is not None and a < b


def eval_greater(a, b):
    if isinstance(a, str) and not isinstance(b, str):
        try:
            return float(a) > b
        except ValueError:
            return False
    return a is not None and a > b


def eval_like(a, b):
    return a is not None and re.search(re.escape(b).replace('%', '.*'), a) is not None


def eval_ilike(a, b):
    return a is not None and re.search(re.escape(b.lower()).replace('%', '.*'), a.lower()) is not None


def eval_is(a, b):
    return a is b


eq_op = ComparisonOperator("=", eval_equal)
less_op = ComparisonOperator("<", eval_lesser)
greater_op = ComparisonOperator(">", eval_greater)
like_op = ComparisonOperator("LIKE", eval_like)
ilike_op = ComparisonOperator("ILIKE", eval_ilike)
is_op = ComparisonOperator("IS", eval_is)


OPERATORS = {"=": eq_op,
             "<": less_op,
             ">": greater_op,
             "LIKE": like_op,
             "ILIKE": ilike_op,
             "IS": is_op}

DERIVED_OPERATORS = {"=": (eq_op, True),
                     "<": (less_op, True),
                     ">=": (less_op, False),
                     ">": (greater_op, True),
                     "<=": (greater_op, False),
                     "LIKE": (like_op, True),
                     "~~": (like_op, True),
                     "ILIKE": (ilike_op, True),
                     "IS": (is_op, True)}
