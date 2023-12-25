

from __future__ import annotations
import json
from random import sample
from threading import Thread
from typing import Dict, List, Optional

from data.wrap_value import wrap_value
from encoder.encoder_util import build_sql_sample, get_samples_sql, SampleBuffer, sql_sample_query
from encoder.entropy_picker import EntropyPicker
from encoder.greedy_feature_selection import greedy_feature_selection
from encoder.sample_entry import SampleEntry
from encoder.sampler import Sampler
import numpy as np
from query.predicate import Predicate, ArbitraryPredicate
from query.query_utility import predicates_to_string
from query.sql.sql_query import SQLQuery
from query.sql.sql_table_instance import SQLTableInstance
from schema.comparison_operator import OPERATORS
from schema.sql.sql_schema import SQLSchema
from schema.sql.sql_table import SQLTable


class SQLSampler(Sampler):
    def __init__(self,
                 schema: SQLSchema,
                 separate: bool,
                 bitmap_size: int,
                 samples: Dict[SQLTable, List[SampleEntry]],
                 fuse_from: Optional[Dict[SQLTable, int]] = None,
                 hedge_fused: bool = False) -> None:
        super().__init__(bitmap_size)
        self._schema = schema
        self._separate = separate
        self._samples = samples
        if fuse_from is not None:
            self._fuse_from = fuse_from
        else:
            self._fuse_from = {}
        self._hedge_fused = hedge_fused
        if separate:
            self._ones = {table: np.ones(self.hetero_bitmap_size(table)) for table in samples}

    def samples(self) -> Dict[SQLTable, List[SampleEntry]]:
        return self._samples

    def hetero_bitmap_sizes(self) -> Dict[str, int]:
        return {table.name(): self.hetero_bitmap_size(table) for table in self._samples}

    def hetero_bitmap_size(self, table: SQLTable) -> int:
        assert(table in self._samples)
        if table in self._fuse_from:
            return self._fuse_from[table] + 1
        else:
            return len(self._samples[table])

    def _bitmap(self, node: SQLTableInstance, alias: Optional[str] = None) -> np.ndarray:
        arbitrary = False
        for disjunction in node.predicates():
            for predicate in disjunction:
                if isinstance(predicate, ArbitraryPredicate):
                    arbitrary = True
                    break

        table = node.table()
        if arbitrary:
            if not self._separate:
                raise NotImplementedError
            if alias is None:
                raise ValueError
            eval_dict = evaluate_arbitrary_predicates(self._schema, self._samples[table], node, alias)
            bits = [float(eval_dict[sample]) for sample in self._samples[table]]
        else:
            if self._separate:
                if len(node.predicates()) == 0:
                    return self._ones[table]
                bits = [float(sample.evaluate_sample_predicates(node)) for sample in self._samples[table]]
            else:
                bits = [float(sample.evaluate_sample(node)) for sample in self._samples[table]]
        if table not in self._fuse_from:
            return np.array(bits)
        else:
            unfused = bits[:self._fuse_from[table]]
            fused = bits[self._fuse_from[table]:]
            fused_average = sum(fused) / len(fused)
            if self._hedge_fused and fused_average == 0 and len(self._samples[table]) < table.cardinality():
                fused_average = 1 / (table.cardinality() - self._fuse_from[table])
            combined = unfused + [fused_average]
            return np.array(combined)

    @staticmethod
    def build_sampler_random(schema: SQLSchema, separate: bool, bitmap_size: int) -> SQLSampler:
        connection = schema.connection()
        cursor = connection.cursor()

        if separate:
            sample_number = bitmap_size
        else:
            sample_number = bitmap_size // len(schema.nodes())

        table_samples = {}

        for table in schema.nodes():
            assert(isinstance(table, SQLTable))
            table_samples[table] = get_samples_sql(cursor, table, sample_number)

        cursor.close()

        if separate:
            samples = table_samples
        else:
            merged = sum([table_samples[table] for table in schema.nodes()], [])
            bitmap_size = len(merged)
            samples = {table: merged for table in schema.nodes()}
        return SQLSampler(schema, separate, bitmap_size, samples)

    @staticmethod
    def build_sampler_entropy(schema: SQLSchema,
                              separate: bool,
                              bitmap_size: int,
                              choice_number: int,
                              queries: List[SQLQuery],
                              buffer_size: int = 100000,
                              min_gain: float = 0) -> SQLSampler:
        connection = schema.connection()
        cursor = connection.cursor()

        print("Sampling started")

        table_instances = {}
        for table in schema.nodes():
            assert(isinstance(table, SQLTable))
            all_instance = SQLTableInstance(table, -1, [])
            none_instance = SQLTableInstance(table, -1, [[Predicate(table.attributes()[0], OPERATORS["IS"], None, positive=True)], [Predicate(table.attributes()[0], OPERATORS["IS"], None, positive=False)]])
            table_instances[table] = {all_instance, none_instance}

        for query in queries:
            for table_instance in query.nodes():
                assert (isinstance(table_instance, SQLTableInstance))
                if len(table_instance.predicates()) > 0:
                    table_instances[table_instance.table()].add(table_instance)

        if separate:
            samples = {}
            for table in schema.nodes():
                assert (isinstance(table, SQLTable))
                samples[table] = []
                entropy_picker = EntropyPicker([table_instances[table]], min_gain=min_gain)
                sample_buffer = SampleBuffer(lambda t=table: get_samples_sql(cursor, t, buffer_size))
                for i in range(bitmap_size):
                    sample_choices = sample_buffer.get_samples(choice_number)
                    sample, gain = entropy_picker.pick(sample_choices)
                    if sample is None:
                        break
                    samples[table].append(sample)
                print("%s: %d samples" % (table.name(), len(samples[table])))
            bitmap_size = max([len(samples[table]) for table in samples])
        else:
            entropy_picker = EntropyPicker([table_instances[table] for table in table_instances], min_gain=min_gain)
            tables = [table for table in table_instances if len(table_instances[table]) > 0]
            table_choices = choice_number // len(tables)
            samples = []
            sample_buffers = {table: SampleBuffer(lambda t=table: get_samples_sql(cursor, t, buffer_size)) for table in tables}
            for i in range(bitmap_size):
                sample_choices = []
                for table in tables:
                    sample_choices += sample_buffers[table].get_samples(table_choices)
                sample, gain = entropy_picker.pick(sample_choices)
                if sample is None:
                    break
                samples.append(sample)
            bitmap_size = len(samples)
            print("%d samples" % len(samples))
            samples = {table: samples for table in schema.nodes()}

        print("Sampling finished")

        cursor.close()
        return SQLSampler(schema, separate, bitmap_size, samples)

    @staticmethod
    def build_sampler_fs(schema: SQLSchema,
                         bitmap_size: int,
                         choice_number: int,
                         instance_number: int,
                         max_fs_choices: int,
                         queries: List[SQLQuery],
                         old_sampler: Optional[SQLSampler] = None) -> SQLSampler:
        table_instances = {}
        aliases = {}
        arbitrary_table_instances = {}
        predicate_table_instances = {}
        for table in schema.nodes():
            all_instance = SQLTableInstance(table, -1, [])
            table_instances[table] = {all_instance}
            predicate_table_instances[table] = set()

        for query in queries:
            for table_instance in query.nodes():
                assert (isinstance(table_instance, SQLTableInstance))
                if len(table_instance.predicates()) > 0:
                    predicate_table_instances[table_instance.table()].add(table_instance)
                    aliases[table_instance] = query.alias(table_instance)

        for table in table_instances:
            table_instances[table] = table_instances[table].union(sample(predicate_table_instances[table], min(instance_number, len(predicate_table_instances[table]))))
            arbitrary_table_instances[table] = []
            for table_instance in table_instances[table]:
                for disjunction in table_instance.predicates():
                    for predicate in disjunction:
                        if isinstance(predicate, ArbitraryPredicate):
                            arbitrary_table_instances[table].append(table_instance)
                            break

        cursor = schema.connection().cursor()
        samples = {}
        for table in table_instances:
            if len(table_instances[table]) <= 1:
                samples[table] = []
                print("%s: %d samples" % (table.name(), len(samples[table])))
                continue
            a_set = set()
            a_list = []
            candidate_dict = {}
            if old_sampler is not None:
                # TODO: add support for arbitrary predicates
                old_samples = old_sampler.samples()[table]
                for old_sample in old_samples:
                    a = np.zeros(len(table_instances[table]))
                    for j, table_instance in enumerate(table_instances[table]):
                        a[j] = old_sample.evaluate_sample_predicates(table_instance)
                    a_tuple = tuple(a)
                    a_set.add(a_tuple)
                    candidate_dict[len(a_list)] = old_sample
                    a_list.append(a)
                old_sample_length = len(old_samples)
            else:
                old_sample_length = 0
            sample_query = sql_sample_query(table, choice_number)
            cursor.execute(sample_query)
            candidates = []
            for sample_row in cursor.fetchall():
                candidate = build_sql_sample(table, sample_row)
                candidates.append(candidate)
            arbitrary_evals = {}
            for table_instance in arbitrary_table_instances[table]:
                arbitrary_evals[table_instance] = {}
            for i, candidate in enumerate(candidates):
                a = np.zeros(len(table_instances[table]))
                for j, table_instance in enumerate(table_instances[table]):
                    if table_instance in arbitrary_evals:
                        if candidate not in arbitrary_evals[table_instance]:
                            arbitrary_evals[table_instance] = evaluate_arbitrary_predicates(schema, candidates, table_instance, aliases[table_instance], begin=i)
                        a[j] = arbitrary_evals[table_instance][candidate]
                    else:
                        a[j] = candidate.evaluate_sample_predicates(table_instance)
                # check if a is all zeros or all ones
                a_sum = np.sum(a)
                if a_sum == 0 or a_sum == len(a):
                    continue
                a_tuple = tuple(a)
                if a_tuple not in a_set:
                    a_set.add(a_tuple)
                    candidate_dict[len(a_list)] = candidate
                    a_list.append(a)
                    if len(a_list) >= max_fs_choices:
                        break
            if len(a_set) == 0:
                samples[table] = []
            else:
                A = np.array(list(a_set)).T
                sample_ids = greedy_feature_selection(A, min(bitmap_size, len(a_set)), old_sample_length=old_sample_length)
                samples[table] = [candidate_dict[i] for i in sample_ids]
            print("%s: %d samples" % (table.name(), len(samples[table])))
        cursor.close()
        bitmap_size = max([len(samples[table]) for table in samples])
        return SQLSampler(schema, True, bitmap_size, samples)

    def save(self, filepath: str):
        if self._separate:
            save_samples = self._samples
            sample_order = {}  # not needed
        else:
            raise NotImplementedError()
            save_samples = {}
            sample_order = {}
            for i, sample in enumerate(self._samples[next(self._samples.keys())]):
                tables = sample.labels()
                assert(len(tables) == 1)
                table = next(tables)
                if table not in save_samples:
                    save_samples[table] = []
                    sample_order[table.name()] = []
                save_samples[table].append(sample)
                sample_order[table.name()].append(i)

        keys = {}
        sample_keys = {}
        for table in save_samples:
            key_columns = table.key_columns()
            keys[table.name()] = [column.name() for column in key_columns]
            sample_keys[table.name()] = []
            for sample in save_samples[table]:
                sample_key = [sample.value(column) for column in key_columns]
                sample_keys[table.name()].append(sample_key)
        json_data = {"separate": self._separate,
                     "bitmap_size": self._bitmap_size,
                     "keys": keys,
                     "sample_keys": sample_keys,
                     "fuse_from": {table.name(): self._fuse_from[table] for table in self._fuse_from},
                     "hedge_fused": self._hedge_fused}
        if not self._separate:
            json_data["sample_order"] = sample_order
        with open(filepath, 'w') as f:
            json.dump(json_data, f)

    @staticmethod
    def load(schema: SQLSchema, filepath: str) -> SQLSampler:
        with open(filepath, 'r') as f:
            json_data = json.load(f)
        separate = json_data["separate"]
        bitmap_size = json_data["bitmap_size"]
        keys = json_data["keys"]
        sample_keys = json_data["sample_keys"]
        fuse_from_name = json_data["fuse_from"]
        fuse_from = {schema.node(table_name): fuse_from_name[table_name] for table_name in fuse_from_name}
        hedge_fused = json_data["hedge_fused"]

        cursor = schema.connection().cursor()

        samples = {}
        for table_name in sample_keys:
            table = schema.node(table_name)
            samples[table] = []
            for sample in sample_keys[table_name]:
                sample_query = "SELECT * FROM %s WHERE %s;" % (table_name, " AND ".join("%s = %s" % (key, str(value)) for key, value in zip(keys[table_name], sample)))
                cursor.execute(sample_query)
                sample_row = cursor.fetchone()
                samples[table].append(build_sql_sample(table, sample_row))
        if not separate:
            sample_order = json_data["sample_order"]
            merged = [None] * bitmap_size
            for table_name in sample_order:
                table = schema.node(table_name)
                for i, sample in zip(sample_order[table_name], samples[table]):
                    merged[i] = sample
            for table in samples:
                samples[table] = merged

        cursor.close()

        return SQLSampler(schema, separate, bitmap_size, samples, fuse_from=fuse_from, hedge_fused=hedge_fused)

def get_fs_samples(schema: SQLSchema, table: SQLTable, table_instances: List[SQLTableInstance], choice_number: int, max_fs_choices: int, bitmap_size: int, result: List[SampleEntry]):
    cursor = schema.connection().cursor()
    sample_query = sql_sample_query(table, choice_number)
    cursor.execute(sample_query)
    a_set = set()
    a_list = []
    candidate_dict = {}
    for sample_row in cursor.fetchall():
        candidate = build_sql_sample(table, sample_row)
        a = np.zeros(len(table_instances))
        for j, table_instance in enumerate(table_instances):
            a[j] = candidate.evaluate_sample_predicates(table_instance)
        # check if a is all zeros or all ones
        a_sum = np.sum(a)
        if a_sum == 0 or a_sum == len(a):
            continue
        a_tuple = tuple(a)
        if a_tuple not in a_set:
            a_set.add(tuple(a))
            candidate_dict[len(a_list)] = candidate
            a_list.append(a)
            if len(a_list) >= max_fs_choices:
                break
    if len(a_set) == 0:
        return
    else:
        A = np.array(list(a_set)).T
        sample_ids = greedy_feature_selection(A, min(bitmap_size, len(a_set)))
        for i in sample_ids:
            result.append(candidate_dict[i])
    cursor.close()


def evaluate_arbitrary_predicates(schema: SQLSchema, samples: List[SampleEntry], table_instance: SQLTableInstance, alias: str, samples_per_query: int = 5000, begin: Optional[int] = None) -> Dict[SampleEntry, bool]:
    if len(samples) == 0:
        return {}
    table = list(samples[0].labels())[0]
    primary_key = table.key_columns()
    if begin is None:
        sample_partitions = [samples[i:i + samples_per_query] for i in range(0, len(samples), samples_per_query)]
    else:
        sample_partitions = [samples[begin:begin + samples_per_query]]
    connection = schema.connection()
    result = {}
    select_string = "SELECT %s FROM %s AS %s" % (predicates_to_string(alias, table_instance.predicates()), table.name(), alias)
    cursor = connection.cursor()
    for samples in sample_partitions:
        sample_strings = []
        for sample in samples:
            sample_key = [sample.value(column) for column in primary_key]
            sample_strings.append("(" + ", ". join([wrap_value(value) for value in sample_key]) + ")")

        in_string = "(" + ", ". join([pk.name() for pk in primary_key]) + ")" + " IN (" + ", ".join(sample_strings) + ")"
        query = select_string + " WHERE " + in_string + ";"

        cursor.execute(query)
        for sample, row in zip(samples, cursor.fetchall()):
            eval = row[0]
            result[sample] = eval is not None and eval
    cursor.close()
    return result

