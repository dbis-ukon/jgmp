

class SchemaNode:
    def __init__(self, name: str, cardinality: int, is_leaf: bool = False) -> None:
        self._name = name
        self._cardinality = cardinality
        self._is_leaf = is_leaf

    def name(self) -> str:
        return self._name

    def cardinality(self) -> int:
        return self._cardinality

    def is_leaf(self) -> bool:
        return self._is_leaf
