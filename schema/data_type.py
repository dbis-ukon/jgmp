
from schema.comparison_operator import ComparisonOperator, OPERATORS
from typing import List
from enum import Enum
from datetime import date, datetime


class EncodingType(Enum):
    NUMERIC = 1
    STRING = 2


class DataType:
    def __init__(self,
                 name: str,
                 operators: List[ComparisonOperator],
                 python_type: type,
                 encoding_type: EncodingType,
                 size: int) -> None:
        self._name = name
        self._operators = operators
        self._python_type = python_type
        self._encoding_type = encoding_type
        self._size = size

    def name(self) -> str:
        return self._name

    def operators(self) -> List[ComparisonOperator]:
        return self._operators

    def python_type(self) -> type:
        return self._python_type

    def encoding_type(self) -> EncodingType:
        return self._encoding_type

    def size(self) -> int:
        return self._size


_integer = DataType("integer",
                    [OPERATORS["="], OPERATORS["<"], OPERATORS[">"], OPERATORS["IS"]],
                    int,
                    EncodingType.NUMERIC,
                    4)
_bigint = DataType("bigint",
                   [OPERATORS["="], OPERATORS["<"], OPERATORS[">"], OPERATORS["IS"]],
                   int,
                   EncodingType.NUMERIC,
                   8)
_numeric = DataType("numeric",
                    [OPERATORS["="], OPERATORS["<"], OPERATORS[">"], OPERATORS["IS"]],
                    float,
                    EncodingType.NUMERIC,
                    8)
_string = DataType("character varying",
                   [OPERATORS["="], OPERATORS["<"], OPERATORS[">"], OPERATORS["LIKE"], OPERATORS["ILIKE"], OPERATORS["IS"]],
                   str,
                   EncodingType.STRING,
                   16)
_date = DataType("date",
                 [OPERATORS["="], OPERATORS["<"], OPERATORS[">"], OPERATORS["IS"]],
                 date,
                 EncodingType.NUMERIC,
                 4)
_datetime = DataType("datetime",
                     [OPERATORS["="], OPERATORS["<"], OPERATORS[">"], OPERATORS["IS"]],
                     datetime,
                     EncodingType.NUMERIC,
                     8)

DATATYPES = {"bigint": _bigint,
             "integer": _integer,
             "smallint": _integer,
             "numeric": _numeric,
             "character": _string,
             "character varying": _string,
             "text": _string,
             "String": _string,
             "Long": _bigint,
             "Date": _date,
             "date": _date,
             "DateTime": _datetime,
             "timestamp without time zone": _datetime}
