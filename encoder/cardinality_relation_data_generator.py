from threading import Thread
import torch
from typing import List, Optional
from torch import LongTensor, BoolTensor
from torch_geometric.loader import DataLoader
from encoder.encoder import Encoder
import random

from models.bulk_mscn_cardinality_model import BulkMSCNCardinalityModel
from query.query_generator import QueryGenerator
from query.symmetry.cardinality_relation import RelationType, CardinalityRelation
from query.symmetry.generator.cardinality_relation_generator import CardinalityRelationGenerator
from query_data.bulk_light_relation_query_data import BulkLightRelationQueryData
from query_data.bulk_relation_query_data import BulkRelationQueryData
from query_data.cardinality_relation_query_data import CardinalityRelationQueryData
from query_data.graphlike_query_data import GraphlikeQueryData
import numpy as np


class QueryServer:
    def __init__(self, query_generator: QueryGenerator, reuse_chance: float):
        self._query_generator = query_generator
        self._reuse_chance = reuse_chance
        self._last_query = None
        self._last_relation = None

    def serve_query(self):
        if self._last_relation is not None and random.random() < self._reuse_chance:
            candidates = []
            for query in self._last_relation.left() + self._last_relation.right():
                if query != self._last_query:
                    candidates.append(query)
            next_query = random.choice(candidates)
        else:
            next_query = self._query_generator.generate_query_default()
        self._last_query = next_query
        return next_query

    def set_last_relation(self, cardinality_relation: Optional[CardinalityRelation]):
        self._last_relation = cardinality_relation


class CardinalityRelationDataGenerator:
    def __init__(self,
                 query_generator: QueryGenerator,
                 encoder: Encoder,
                 relation_generators: List[CardinalityRelationGenerator],
                 device: torch.device,
                 reuse_chance: float = 0.5) -> None:
        self._query_generator = query_generator
        self._encoder = encoder
        self._relation_generators = relation_generators
        self._device = device
        self._reuse_chance = reuse_chance

    def generate_cardinality_relations_parallel(self,
                                                num_relations: int,
                                                num_threads: int = 8
                                                ) -> List[CardinalityRelationQueryData]:
        num_divided = num_relations // num_threads
        num_modulo = num_relations % num_threads
        results = [None] * num_threads
        threads = []
        for i in range(num_threads):
            if i < num_modulo:
                thread_num = num_divided + 1
            else:
                thread_num = num_divided
            thread = Thread(target=generate_relations, args=(self, thread_num, results, i))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        return sum(results, [])

    def generate_cardinality_relations(self,
                                       num_relations: int
                                       ) -> List[CardinalityRelationQueryData]:
        query_server = QueryServer(self._query_generator, self._reuse_chance)
        relations = []
        while len(relations) < num_relations:
            query_data: List[GraphlikeQueryData] = []
            left = [[], []]
            right = [[], []]
            cardinality_relation = None
            while cardinality_relation is None:
                query = query_server.serve_query()
                relation_generator = random.choice(self._relation_generators)
                cardinality_relation = relation_generator.generate(query)
                query_server.set_last_relation(cardinality_relation)
            for query in cardinality_relation.left():
                left[0].append(len(query_data))
                left[1].append(0)
                query_data.append(self._encoder.encode_graphlike_query(query))
            for query in cardinality_relation.right():
                right[0].append(len(query_data))
                right[1].append(0)
                query_data.append(self._encoder.encode_graphlike_query(query))
            equal = [cardinality_relation.type() == RelationType.EQUAL]
            batched = next(iter(DataLoader(query_data, batch_size=len(query_data))))
            crqd = CardinalityRelationQueryData(batched.x,
                                                batched.edge_index,
                                                batched.edge_attr,
                                                batched.edge_directions,
                                                batched.edge_directions_reversed,
                                                batched.node_predicates,
                                                batched.node_disjunction_index,
                                                batched.node_conjunction_index,
                                                batched.edge_predicates,
                                                batched.edge_disjunction_index,
                                                batched.edge_conjunction_index,
                                                batched.node_count,
                                                LongTensor(np.stack(left)),
                                                LongTensor(np.stack(right)),
                                                BoolTensor(equal),
                                                batched.batch)
            crqd.to(self._device, non_blocking=True)
            relations.append(crqd)

        return relations

    def generate_bulk_cardinality_relations_parallel(self,
                                                     num_relations: int,
                                                     num_threads: int = 8
                                                     ) -> List[BulkRelationQueryData]:
        num_divided = num_relations // num_threads
        num_modulo = num_relations % num_threads
        results = [None] * num_threads
        threads = []
        for i in range(num_threads):
            if i < num_modulo:
                thread_num = num_divided + 1
            else:
                thread_num = num_divided
            thread = Thread(target=generate_bulk_relations, args=(self, thread_num, results, i))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        return sum(results, [])

    def generate_bulk_cardinality_relations(self,
                                            num_relations: int
                                            ) -> List[BulkRelationQueryData]:
        query_server = QueryServer(self._query_generator, self._reuse_chance)
        relations = []
        while len(relations) < num_relations:
            cardinality_relation = None
            while cardinality_relation is None:
                query = query_server.serve_query()
                relation_generator = random.choice(self._relation_generators)
                cardinality_relation = relation_generator.generate(query)
                query_server.set_last_relation(cardinality_relation)
            crqd = self._encoder.bulk_encode_relation(cardinality_relation)
            crqd.to(self._device, non_blocking=True)
            relations.append(crqd)

        return relations

    def generate_bulk_light_cardinality_relations_parallel(self,
                                                           num_relations: int,
                                                           num_threads: int = 8
                                                           ) -> List[BulkRelationQueryData]:
        num_divided = num_relations // num_threads
        num_modulo = num_relations % num_threads
        results = [None] * num_threads
        threads = []
        for i in range(num_threads):
            if i < num_modulo:
                thread_num = num_divided + 1
            else:
                thread_num = num_divided
            thread = Thread(target=generate_bulk_light_relations, args=(self, thread_num, results, i))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        return sum(results, [])

    def generate_bulk_light_cardinality_relations(self,
                                                  num_relations: int
                                                  ) -> List[BulkRelationQueryData]:
        query_server = QueryServer(self._query_generator, self._reuse_chance)
        relations = []
        while len(relations) < num_relations:
            cardinality_relation = None
            while cardinality_relation is None:
                query = query_server.serve_query()
                relation_generator = random.choice(self._relation_generators)
                cardinality_relation = relation_generator.generate(query)
                query_server.set_last_relation(cardinality_relation)
            crqd = self._encoder.bulk_light_encode_relation(cardinality_relation)
            crqd.to(self._device, non_blocking=True)
            relations.append(crqd)

        return relations

    def generate_bulk_mscn_cardinality_relations_parallel(self,
                                                          num_relations: int,
                                                          num_threads: int = 8
                                                          ) -> List[BulkRelationQueryData]:
        num_divided = num_relations // num_threads
        num_modulo = num_relations % num_threads
        results = [None] * num_threads
        threads = []
        for i in range(num_threads):
            if i < num_modulo:
                thread_num = num_divided + 1
            else:
                thread_num = num_divided
            thread = Thread(target=generate_bulk_mscn_relations, args=(self, thread_num, results, i))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        return sum(results, [])

    def generate_bulk_mscn_cardinality_relations(self,
                                                 num_relations: int
                                                 ) -> List[BulkRelationQueryData]:
        query_server = QueryServer(self._query_generator, self._reuse_chance)
        relations = []
        while len(relations) < num_relations:
            cardinality_relation = None
            while cardinality_relation is None:
                query = query_server.serve_query()
                if not BulkMSCNCardinalityModel.supports(query):
                    continue
                relation_generator = random.choice(self._relation_generators)
                cardinality_relation = relation_generator.generate(query)
                query_server.set_last_relation(cardinality_relation)
            crqd = self._encoder.bulk_mscn_encode_relation(cardinality_relation)
            crqd.to(self._device, non_blocking=True)
            relations.append(crqd)

        return relations


def generate_relations(generator: CardinalityRelationDataGenerator, num_relations: int, results: List[Optional[List[CardinalityRelationQueryData]]], result_index: int):
    results[result_index] = generator.generate_cardinality_relations(num_relations)

def generate_bulk_relations(generator: CardinalityRelationDataGenerator, num_relations: int, results: List[Optional[List[BulkRelationQueryData]]], result_index: int):
    results[result_index] = generator.generate_bulk_cardinality_relations(num_relations)

def generate_bulk_light_relations(generator: CardinalityRelationDataGenerator, num_relations: int, results: List[Optional[List[BulkLightRelationQueryData]]], result_index: int):
    results[result_index] = generator.generate_bulk_light_cardinality_relations(num_relations)

def generate_bulk_mscn_relations(generator: CardinalityRelationDataGenerator, num_relations: int, results: List[Optional[List[BulkLightRelationQueryData]]], result_index: int):
    results[result_index] = generator.generate_bulk_mscn_cardinality_relations(num_relations)