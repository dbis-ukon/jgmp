
from typing import Dict, FrozenSet, List, Optional, Set, Tuple
from query.query_node import QueryNode
from encoder.sample_entry import SampleEntry
import numpy as np

from schema.schema_node import SchemaNode


class EntropyPicker:
    def __init__(self, node_lists: List[List[QueryNode]], min_gain: float = 0) -> None:
        self._node_sets = [frozenset(node_list) for node_list in node_lists if len(node_list) > 0]
        self._min_gain = min_gain

    def pick(self, samples: List[SampleEntry]) -> Tuple[Optional[SampleEntry], float]:
        if len(self._node_sets) == 0:
            return None, 0
        winner = None

        maximum_gain = 0
        label_index = EntropyPicker._label_index(self._node_sets)

        for sample in samples:
            node_set_sets = []
            for label in sample.labels():
                if label in label_index:
                    node_set_sets.append(label_index[label])
                else:
                    node_set_sets.append(set())
                    break
            node_set_set = set.intersection(*node_set_sets)
            gain = 0
            for node_set in node_set_set:
                gain += EntropyPicker._information_gain(node_set, sample)
            if gain > maximum_gain and gain > self._min_gain:
                winner = sample
                maximum_gain = gain

        if maximum_gain > 0:
            self._node_sets = EntropyPicker._divide(self._node_sets, winner)

        return winner, maximum_gain

    @staticmethod
    def _get_entropy(node_sets: List[FrozenSet[QueryNode]]) -> float:
        entropy = 0
        for node_set in node_sets:
            entropy += EntropyPicker._entropy_contribution(node_set)

        return entropy

    @staticmethod
    def _entropy_contribution(node_set: FrozenSet[QueryNode]) -> float:
        return len(node_set) * np.log2(len(node_set))

    @staticmethod
    def _divide(node_sets: List[FrozenSet[QueryNode]], sample_entry: SampleEntry) -> List[FrozenSet[QueryNode]]:
        new_node_sets = []
        for node_set in node_sets:
            hits, misses = EntropyPicker._divide_set(node_set, sample_entry)
            for new_node_set in [hits, misses]:
                if len(new_node_set) > 0:
                    new_node_sets.append(frozenset(new_node_set))
        return new_node_sets

    @staticmethod
    def _divide_set(node_set: FrozenSet[QueryNode], sample_entry: SampleEntry) -> Tuple[List[QueryNode], List[QueryNode]]:
        hits = []
        misses = []
        for node in node_set:
            if sample_entry.evaluate_sample(node):
                hits.append(node)
            else:
                misses.append(node)
        return hits, misses

    @staticmethod
    def _information_gain(node_set: FrozenSet[QueryNode], sample_entry: SampleEntry) -> float:
        hits, misses = EntropyPicker._divide_set(node_set, sample_entry)
        if len(hits) == 0 or len(misses) == 0:
            return 0
        return EntropyPicker._entropy_contribution(node_set) - EntropyPicker._entropy_contribution(hits) - EntropyPicker._entropy_contribution(misses)

    @staticmethod
    def _label_index(node_sets: List[FrozenSet[QueryNode]]) -> Dict[SchemaNode, Set[FrozenSet[QueryNode]]]:
        label_index = {}
        for node_set in node_sets:
            for node in node_set:
                for label in node.labels():
                    if label not in label_index:
                        label_index[label] = set()
                    label_index[label].add(node_set)
        return label_index
