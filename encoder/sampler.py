
from __future__ import annotations
from abc import abstractmethod
from typing import Optional

import numpy as np
from query.query_node import QueryNode


class Sampler:
    def __init__(self, bitmap_size: int) -> None:
        self._bitmap_size = bitmap_size
        self._bitmaps = {}
        self._bitmaps_padded = {}

    def bitmap_size(self) -> int:
        return self._bitmap_size

    def bitmap_padded(self, node: QueryNode, alias: Optional[str] = None) -> np.ndarray:
        if node not in self._bitmaps_padded:
            bitmap = self.bitmap(node, alias=alias)
            self._bitmaps_padded[node] = np.pad(bitmap, (0, self._bitmap_size - len(bitmap)))
        return self._bitmaps_padded[node]

    def bitmap(self, node: QueryNode, alias: Optional[str] = None) -> np.ndarray:
        if node not in self._bitmaps:
            self._bitmaps[node] = self._bitmap(node, alias=alias)
        return self._bitmaps[node]

    @abstractmethod
    def _bitmap(self, node: QueryNode, alias: Optional[str] = None) -> np.ndarray:
        pass

    def reset(self):
        self._bitmaps = {}
        self._bitmaps_padded = {}

    @abstractmethod
    def save(self, filepath: str):
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def load(filepath: str) -> Sampler:
        raise NotImplementedError()
