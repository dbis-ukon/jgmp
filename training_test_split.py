

from typing import Dict, List, Tuple, TypeVar
import random
import numpy as np
from query.sql.sql_query import SQLQuery
from collections import defaultdict


T = TypeVar('T')


def classic_split(test_ratio: float, objects: List[T]) -> Tuple[List[T], List[T]]:
    rand = random.Random(42)
    rand.shuffle(objects)
    split = int(test_ratio * len(objects))
    return objects[split:], objects[:split]


def group_job(query_names: Dict[SQLQuery, str]) -> List[List[SQLQuery]]:
    groups = {}
    for query in query_names:
        group_name = query_names[query][:-1]
        if group_name not in groups:
            groups[group_name] = []
        groups[group_name].append(query)
    return [groups[name] for name in groups]


def grouped_split(test_ratio: float, objects: List[List[T]]) -> Tuple[List[T], List[T]]:
    rand = random.Random(42)
    rand.shuffle(objects)
    split = int(test_ratio * sum([len(object) for object in objects]))
    test = []
    training = []
    for group in objects:
        if len(test) < split:
            test += group
        else:
            training += group
    return training, test


def multi_split(repetitions: int, num_parts: int, objects: List[T]) -> List[Tuple[List[T], List[T]]]:
    rand = random.Random(42)
    partitions = []
    for _ in range(repetitions):
        index_array = np.array(range(len(objects)))
        rand.shuffle(index_array)
        parts = [list(part) for part in np.array_split(index_array, num_parts)]
        for i, part in enumerate(parts):
            part_set = set(part)
            train = []
            test = []
            for i, object in enumerate(objects):
                if i in part_set:
                    test.append(object)
                else:
                    train.append(object)
            partitions.append((train, test))
    return partitions


def grouped_multi_split(repetitions: int, num_parts: int, objects: List[List[T]]) -> List[Tuple[List[T], List[T]]]:
    partitions = multi_split(repetitions, num_parts, objects)
    new_partitions = []
    for left, right in partitions:
        new_left = [element for group in left for element in group]
        new_right = [element for group in right for element in group]
        new_partitions.append((new_left, new_right))
    return new_partitions


def incremental_subsets(repetitions: int, subset_sizes: List[int], objects: List[T]) -> List[List[Tuple[List[T], List[T]]]]:
    subsets = []
    subset_sizes = sorted(subset_sizes, reverse=True)
    for subset_size in subset_sizes:
        if subset_size > len(objects):
            continue
        current_subsets = []
        balanced_subsets = balanced_random_subsets(repetitions, subset_size, objects)
        for balanced_subset in balanced_subsets:
            opposite = [object for object in objects if object not in balanced_subset]
            current_subsets.append((balanced_subset, opposite))
        subsets.append(current_subsets)
    return subsets


def balanced_random_subsets(repetitions: int, subset_size: int, superset: List[T]) -> List[List[T]]:
    rand = random.Random(42)
    superset_list = list(superset)
    num_elements = len(superset_list)

    if subset_size > num_elements:
        raise ValueError("Subset size (m) should be less than or equal to the size of the superset.")

    # Keep track of the count of each element in the subsets
    element_counts = defaultdict(int)

    subsets = []
    # Distribute elements evenly across the subsets
    for _ in range(repetitions):
        current_count = 0
        subset = set()

        rand.shuffle(superset_list)
        set_iter = iter(superset_list)
        while len(subset) < subset_size:
            try:
                element = next(set_iter)
            except StopIteration:
                rand.shuffle(superset_list)
                set_iter = iter(superset_list)
                element = next(set_iter)
                current_count += 1
            if element_counts[element] == current_count and element not in subset:
                subset.add(element)
                element_counts[element] += 1
        subsets.append(list(subset))

    return subsets


