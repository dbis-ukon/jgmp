import math
from enum import Enum
from cardinality_configs import selfsupervised_factor_experiment_configs, jgmp_config, mscn_config, \
    mscn_hybrid_config, default_configs, semisupervised_experiment_configs, fs_configs, feature_selection_ablation_configs, incremental_configs, \
    fk_direction_ablation_configs, redo_incremental_configs
from cardinality_estimator.cardinality_estimator import CardinalityEstimator
from cardinality_estimator.learned_cardinality_estimator import LearnedCardinalityEstimator
from cardinality_estimator.prophetic_cardinality_estimator import PropheticCardinalityEstimator
from cardinality_injection import get_runtime_sql
from data.query_db import QueryDB
from encoder.cardinality_relation_data_generator import CardinalityRelationDataGenerator
from encoder.sql_sampler import SQLSampler
from models.bulk_jgmp_cardinality_model import BulkJGMPCardinalityModel
from models.bulk_mscn_cardinality_model import BulkMSCNCardinalityModel
from models.cardinality_model import CardinalityModel
from models.graph_cardinality_model import GraphCardinalityModel
from models.query_model import QueryModel
from plan_execution.pg_explain import PgExplain
from plan_execution.stubborn_plan_engine import StubbornPlanEngine
from query.query_generator import QueryGenerator
from query.query_generator_choice import QueryGeneratorChoice
from query.query_node import QueryNode
from query.sql.sql_query import SQLQuery
from query.graphlike_query import GraphlikeQuery
from query.symmetry.generator.card_rel_complementarity import CardRelComplementarity
from query.symmetry.generator.card_rel_foreign_key import CardRelForeignKey
from query.symmetry.generator.card_rel_greater_monotonicity import CardRelGreaterMonotonicity
from query.symmetry.generator.card_rel_inclusion_exclusion import CardRelInclusionExclusion
from query.symmetry.generator.card_rel_or_monotonicity import CardRelOrMonotonicity
from query_data.bulk_cardinality_query_data import BulkCardinalityQueryData
from schema.graphlike_schema import GraphlikeSchema
from typing import Any, Dict, List, Optional, Tuple, Union, FrozenSet
from encoder.encoder import Encoder
from schema.schemas import stats_schema, imdb_schema, imdb_light_schema
from schema.sql.sql_random_attribute_buffer import SQLRandomAttributeBuffer
from train_cardinality import encode, test, train, q_error_stats
from training_test_split import group_job, multi_split, incremental_subsets
from schema.sql.sql_schema import SQLSchema
from logger import Logger
import random
import numpy as np
import hyperopt
import torch
import datetime
import os
import sys

from utility import load_learned_estimator_sql


class SubqueryMode(Enum):
    NONE = 1
    BEST = 2
    ALL = 3

class TrainingSetup:
    def __init__(self,
                 schema: GraphlikeSchema,
                 train_and_validate: List[List[GraphlikeQuery]],
                 query_generator: Optional[QueryGenerator],
                 tests: List[Tuple[str, List[List[GraphlikeQuery]]]],
                 config: Dict[str, Any],
                 validate_ratio: float,
                 old_model: Optional[CardinalityModel] = None):
        self.schema = schema
        self.train_and_validate = train_and_validate
        self.query_generator = query_generator
        self.tests = tests
        self.config = config
        self.validate_ratio = validate_ratio
        self.old_model = old_model


class TrainingResult:
    def __init__(self,
                 estimator: CardinalityEstimator,
                 setup: TrainingSetup):
        self.estimator = estimator
        self.setup = setup



def distribute_subplans(training_set_pure: List[List[SQLQuery]], max_queries: int = 1000) -> List[List[SQLQuery]]:
    new_training_set_pure = []
    for subplan_queries in training_set_pure:
        # split queries with many subplans into multiple chunks to avoid memory issues
        num_chunks = math.ceil(len(subplan_queries) / max_queries)
        chunk_size = math.ceil(len(subplan_queries) / num_chunks)
        for i in range(num_chunks):
            new_training_set_pure.append(subplan_queries[i * chunk_size:(i + 1) * chunk_size])
    return new_training_set_pure


def count_parameters(model):
    return sum([p.numel() for p in model.parameters() if p.requires_grad])


def experiment(schema: GraphlikeSchema,
               query_db: QueryDB,
               setups: List[TrainingSetup],
               tests: List[Tuple[str, bool, List[List[GraphlikeQuery]]]],
               device: torch.device,
               experiment_type: Optional[str] = None):
    date_string = datetime.datetime.now().isoformat(timespec="seconds")
    dir_path = os.path.dirname(os.path.realpath(__file__))
    result_path = dir_path + "/" + "results/cardinality_estimation/experiment_" + date_string
    if experiment_type is not None:
        result_path += "_" + experiment_type
    result_path += "/"
    os.makedirs(result_path, exist_ok=True)
    group_counters = {}
    results = []
    for setup in setups:
        group_path = result_path + setup.config["name"] + "/"
        if setup.config["name"] not in group_counters:
            group_counters[setup.config["name"]] = 0
            os.mkdir(group_path)
        setup_path = group_path + str(group_counters[setup.config["name"]])
        sys.stdout = Logger(setup_path + ".log")
        _, encoder, model = train_test(setup.schema, setup.train_and_validate, setup.tests, device, setup.config, validate_ratio=setup.validate_ratio, query_generator = setup.query_generator, old_model = setup.old_model)
        sys.stdout = sys.__stdout__
        model.to("cpu")
        estimator = LearnedCardinalityEstimator(model, device="cpu")
        estimator.save(setup_path)
        results.append(TrainingResult(estimator, setup))
        group_counters[setup.config["name"]] += 1

    for test_name, get_runtimes, test_queries in tests:
        final_card_test(query_db, results, test_name, result_path)

        if get_runtimes:
            final_runtime_test(schema, results, test_name, result_path)


def final_card_test(query_db: QueryDB,
                    results: List[TrainingResult],
                    test_name: str,
                    result_path: str):
    setup_groups = {}
    for result in results:
        if result.setup.config["name"] not in setup_groups:
            setup_groups[result.setup.config["name"]] = []
        setup_groups[result.setup.config["name"]].append(result)

    for group in setup_groups:
        card_file_path = result_path + group + "/" + test_name + "_card_est.csv"
        print()
        print("Saving to " + card_file_path)
        card_est_file = open(card_file_path, 'w')
        for model_number, result in enumerate(setup_groups[group]):
            for tn, tq in result.setup.tests:
                if tn == test_name:
                    estimations = result.estimator.bulk_estimate(tq)
                    for q, e in zip(sum(tq, []), estimations):
                        card_est_file.write(str(q.id()) + "," + str(model_number) + "," + str(e) + "\n")
        card_est_file.close()

        calculate_q_errors(query_db, card_file_path)
        print()


def calculate_q_errors(query_db: QueryDB, card_file_path: str):
    estimations = {}
    card_est_file = open(card_file_path, 'r')

    for line in card_est_file.readlines():
        split = line.split(",")
        if len(split) != 3:
            continue
        id, model_number, estimation = split
        id = int(id)
        estimation = float(estimation)
        if id not in estimations:
            estimations[id] = []
        estimations[id].append(estimation)
    card_est_file.close()

    id_cardinalities = query_db.load_cardinities_ids(list(estimations.keys()))
    cardinalities = {}
    for id, cardinality in id_cardinalities:
        cardinalities[id] = cardinality

    labels = []
    predictions = []
    for id in estimations:
        true_card = cardinalities[id]
        for estimation in estimations[id]:
            labels.append(true_card)
            predictions.append(estimation)
    q_error_stats(predictions, labels)


def final_runtime_test(schema: GraphlikeSchema,
                       results: List[TrainingResult],
                       test_name: str,
                       result_path: str,
                       parallel: bool = True,
                       timeout: Optional[int] = 3 * 60 * 60):
    if isinstance(schema, SQLSchema):
        plan_engine = StubbornPlanEngine(schema, not parallel, count=True, timeout=timeout)
    else:
        raise NotImplementedError()

    if parallel:
        parallel_string = "_parallel"
    else:
        parallel_string = ""

    setup_groups = {}
    for result in results:
        if result.setup.config["name"] not in setup_groups:
            setup_groups[result.setup.config["name"]] = []
        setup_groups[result.setup.config["name"]].append(result)

    queries = set()
    for group in setup_groups:
        runtime_file_path = result_path + group + "/" + test_name + parallel_string + "_runtimes.csv"
        print()
        print("Saving to " + runtime_file_path)
        runtime_file = open(runtime_file_path, 'w')
        for model_number, result in enumerate(setup_groups[group]):
            for tn, tq in result.setup.tests:
                if tn == test_name:
                    for query in tq:
                        for subquery in query:
                            queries.add(subquery)
                            if isinstance(schema, SQLSchema):
                                estimation_time, runtime = get_runtime_sql(schema, subquery, result.estimator, plan_engine)
                            else:
                                raise NotImplementedError()
                            line = str(subquery.id()) + "," + str(model_number) + "," + str(estimation_time) + "," + str(runtime)
                            print(line)
                            runtime_file.write(line + "\n")
        runtime_file.close()
        print()

    baseline_runtimes(test_name, list(queries), result_path, plan_engine)


def repeat_runtime_parallel(schema: SQLSchema, path: str, test_name: str, configs: List[Dict[str, Any]] = [], timeout: Optional[int] = 3 * 60 * 60):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    config_dict = {}
    for config in configs:
        config_dict[config["name"]] = config
    plan_engine = StubbornPlanEngine(schema, False, count=True, timeout=timeout)
    queries, query_ids, _, _, _, _, _, group_ids = query_db.load_group_cardinalities(schema, test_name, 2, True, all_group_ids=True)
    id_queries = {}
    for query in queries:
        id_queries[query_ids[query]] = query
    id_groups = {}
    for query in group_ids:
        for group in group_ids[query]:
            for group_id in group_ids[query][group]:
                id_groups[group_id] = group
    for f in os.scandir(path):
        if not f.is_dir():
            continue
        config_directory = f.path
        config_name = f.name
        runtime_file = config_directory + "/" + test_name + "_runtimes.csv"
        if not os.path.exists(runtime_file):
            continue
        estimation_times = {}
        with open(runtime_file, 'r') as f:
            for line in f.readlines():
                split = line.split(",")
                query_id, model_number, estimation_time, _ = split
                query_id = int(query_id)
                model_number = int(model_number)
                estimation_time = float(estimation_time)
                if model_number not in estimation_times:
                    estimation_times[model_number] = {}
                estimation_times[model_number][query_id] = estimation_time
        cardinality_estimators = {}
        if config_name in config_dict:
            config = config_dict[config_name]
            for model_number in estimation_times:
                model_path = config_directory + "/"  + str(model_number)
                cardinality_estimators[model_number] = load_learned_estimator_sql(schema, model_path, config, device=device)
        else:
            cardinality_estimation_file = config_directory + "/" + test_name + "_subqueries_card_est.csv"
            cardinality_estimations = {}
            with open(cardinality_estimation_file, 'r') as f:
                for line in f.readlines():
                    split = line.split(",")
                    query_id, model_number, estimation = split
                    query_id = int(query_id)
                    model_number = int(model_number)
                    estimation = float(estimation)
                    if model_number not in cardinality_estimations:
                        cardinality_estimations[model_number] = {}
                    cardinality_estimations[model_number][query_id] = estimation
            for model_number in cardinality_estimations:
                estimations = {}
                for query_id in cardinality_estimations[model_number]:
                    estimations[id_groups[query_id]] = cardinality_estimations[model_number][query_id]
                cardinality_estimators[model_number] = PropheticCardinalityEstimator(schema, {0: estimations})
        if len(cardinality_estimators) > len(estimation_times):
            continue
        parallel_runtime_file = config_directory + "/" + test_name + "_parallel_runtimes.csv"
        print("Saving to " + parallel_runtime_file)
        with open(parallel_runtime_file, 'w') as f:
            for model_number in cardinality_estimators:
                cardinality_estimator = cardinality_estimators[model_number]
                for query_id in estimation_times[model_number]:
                    query = id_queries[query_id]
                    estimation_time = estimation_times[model_number][query_id]
                    parallel_estimation_time, parallel_runtime = get_runtime_sql(schema, query, cardinality_estimator, plan_engine)
                    if isinstance(cardinality_estimator, PropheticCardinalityEstimator):
                        line = str(query_id) + "," + str(model_number) + "," + str(estimation_time) + "," + str(parallel_runtime)
                    else:
                        line = str(query_id) + "," + str(model_number) + "," + str(parallel_estimation_time) + "," + str(parallel_runtime)
                    print(line)
                    f.write(line + "\n")

    baseline_runtimes(test_name, queries, path, plan_engine, file_name_addition="_parallel")


def load_and_get_runtimes(schema: GraphlikeSchema,
                          query_db: QueryDB,
                          experiment_path: str,
                          test_name: str,
                          estimator_id: int,
                          configs: List[Dict[str, Any]],
                          num_queries: Optional[int] = None,
                          num_models: Optional[int] = None):
    rand = random.Random(42)
    # load test queries
    queries = query_db.load_query_set(schema, test_name, estimator_id)
    if num_queries is not None and num_queries < len(queries):
        queries = rand.sample(queries, num_queries)
    # build config dict
    config_dict = {}
    for config in configs:
        config_dict[config["name"]] = config
    # search experiment directory for subdirectories
    # each subdirectory is a setup
    training_results = []
    for setup_name in os.listdir(experiment_path):
        if setup_name not in config_dict:
            continue
        setup_path = experiment_path + "/" + setup_name + "/"
        test_dict = {}
        # every .pt file in the setup directory is a model
        for model_name in os.listdir(setup_path):
            if model_name.endswith(".pt") :
                model_name = model_name[:-3]
                model_id = int(model_name)
                model_path = setup_path + model_name
                if isinstance(schema, SQLSchema):
                    estimator = load_learned_estimator_sql(schema, model_path, config_dict[setup_name], device="cpu")
                else:
                    raise NotImplementedError()
                tests = [(test_name, [])]
                test_dict[model_id] = tests[0][1]
                setup = TrainingSetup(schema, [], None, tests, config_dict[setup_name], 0)
                training_results.append(TrainingResult(estimator, setup))

        # load cardinality file and build dict mapping queries to list of models with cardinality estimations
        card_file_path = experiment_path + "/" + setup_name + "/" + test_name + "_card_est.csv"
        card_est_file = open(card_file_path, 'r')
        cross_validation_test_sets = {}
        for line in card_est_file.readlines():
            split = line.split(",")
            if len(split) != 3:
                continue
            id, model_number, _ = split
            id = int(id)
            model_number = int(model_number)
            if id not in cross_validation_test_sets:
                cross_validation_test_sets[id] = []
            cross_validation_test_sets[id].append(model_number)

        for query in queries:
            model_ids = cross_validation_test_sets[query.id()]
            if num_models is not None and num_models < len(model_ids):
                model_ids = rand.sample(model_ids, num_models)
            for model_id in model_ids:
                test_dict[model_id].append([query])
    final_runtime_test(schema, training_results, test_name, experiment_path + "/")


def baseline_runtimes(test_name: str, queries: List[Union[SQLQuery, Tuple[str, str]]], result_path: str, plan_engine: StubbornPlanEngine, file_name_addition: str = ""):
    print()
    if result_path[-1] != "/":
        result_path += "/"
    runtime_file_path = result_path + test_name + "_baseline_runtimes%s.csv" % file_name_addition
    print("Saving to " + runtime_file_path)
    runtime_file = open(runtime_file_path, 'w')
    for element in queries:
        if isinstance(element, tuple):
            name, query = element
        else:
            query = element
            name = str(query.id())
        settings = ["SET ml_cardest_enabled = FALSE;",
                    "SET ml_joinest_enabled = FALSE;"]
        execution_result = plan_engine.execute_with_settings(query, None, settings)
        if execution_result is not None:
            _, runtime, _, _ = execution_result
            runtime = runtime / 1000
        else:
            runtime = None
        line = name + "," + str(0) + "," + str(0) + "," + str(runtime)
        print(line)
        runtime_file.write(line + "\n")
    runtime_file.close()


def competitor_runtimes(schema: SQLSchema,
                        query_db: QueryDB,
                        test_name: str,
                        result_path: str,
                        competitor: str,
                        single_table: bool,
                        plan_engine: StubbornPlanEngine):
    queries, _, _, group_cardinalities, _, _, _, _ = query_db.load_group_cardinalities(schema, test_name, 2, True, query_cardinality_estimator=competitor)
    estimator = PropheticCardinalityEstimator(schema, group_cardinalities)
    print()
    if single_table:
        single_table_string = "all"
    else:
        single_table_string = "only_multi_table"
    runtime_file_path = "%s%s_%s_%s_runtimes.csv" % (result_path, test_name, competitor, single_table_string)
    print("Saving to " + runtime_file_path)
    runtime_file = open(runtime_file_path, 'w')
    for query in queries:
        estimation_time, runtime = get_runtime_sql(schema, query, estimator, plan_engine, single_table=single_table)
        line = str(query.id()) + "," + str(0) + "," + str(0) + "," + str(runtime)
        print(line)
        runtime_file.write(line + "\n")
    runtime_file.close()


def get_competitors(schema: SQLSchema,
                    query_db: QueryDB,
                    test_name: str,
                    competitors: List[Tuple[str, bool]],
                    timeout: int = 3 * 60 * 60,
                    disable_gather: bool = False,
                    disable_nested_loop: bool = False):
    date_string = datetime.datetime.now().isoformat(timespec="seconds")
    directory_name_addition = ""
    if not disable_gather:
        directory_name_addition += "_parallel"
    if disable_nested_loop:
        directory_name_addition += "_noloops"
    dir_path = os.path.dirname(os.path.realpath(__file__))
    result_path = dir_path + "/" + "results/cardinality_estimation/competitors_%s%s_%s/" % (test_name, directory_name_addition, date_string)
    os.makedirs(result_path, exist_ok=True)
    plan_engine = StubbornPlanEngine(schema, disable_gather, count=True, timeout=timeout, disable_nested_loop=disable_nested_loop)
    for competitor, single_table in competitors:
        competitor_runtimes(schema, query_db, test_name, result_path, competitor, single_table, plan_engine)
    queries = query_db.load_query_set(schema, test_name, 2)
    baseline_runtimes(test_name, queries, result_path, plan_engine)


def load_train_test(query_db: QueryDB,
                    schema: GraphlikeSchema,
                    train_and_validate_name: str,
                    test_names: List[str],
                    device: torch.device,
                    estimator_id: int,
                    config: Dict[str, Any],
                    semi_supervised: bool) -> Tuple[float, Encoder, GraphCardinalityModel]:
    train_and_validate = query_db.load_query_set(schema, train_and_validate_name, estimator_id, cardinality=True, only_with_true_cardinality=True)
    tests = []
    for test_name in test_names:
        test = query_db.load_query_set(schema, test_name, estimator_id, cardinality=True, only_with_true_cardinality=True)
        tests.append((test_name, test))
    return train_test(schema, train_and_validate, tests, device, config=config, semi_supervised=semi_supervised, validate_ratio=0)


def build_encoder(schema: GraphlikeSchema, config: Dict[str, Any], training_queries: List[GraphlikeQuery]) -> Encoder:
    if isinstance(schema, SQLSchema):
        if config["samples_random"]:
            sampler = SQLSampler.build_sampler_random(schema, config["samples_separate"], config["bitmap_size"])
        else:
            queries = sum(training_queries, [])
            if "min_gain" in config:
                min_gain = config["min_gain"]
            else:
                min_gain = 0
            # sampler = SQLSampler.build_sampler_entropy(schema, config["samples_separate"], config["bitmap_size"], 10000, queries, min_gain=min_gain)
            sampler = SQLSampler.build_sampler_fs(schema, config["bitmap_size"], 100000, 1000, 1000, queries)
    else:
        return NotImplementedError()
    if "eliminate_lesser" in config:
        return Encoder(schema, sampler, eliminate_lesser=config["eliminate_lesser"])
    return Encoder(schema, sampler)


def train_test(schema: GraphlikeSchema,
               train_and_validate: List[List[GraphlikeQuery]],
               tests: List[Tuple[str, List[List[GraphlikeQuery]]]],
               device: torch.device,
               config: Dict[str, Any],
               validate_ratio: float = 0.1,
               query_generator: Optional[QueryGenerator] = None,
               old_model: Optional[GraphCardinalityModel] = None
               ) -> Tuple[float, Encoder, GraphCardinalityModel]:
    print()
    if config is not None:
        print(config)
    random.seed(42)
    random.shuffle(train_and_validate)
    train_and_validate = [[subplan_query for subplan_query in query if config['type'].supports(subplan_query)] for query in train_and_validate]
    train_and_validate = distribute_subplans(train_and_validate)

    if validate_ratio > 0:
        split = int((1 - validate_ratio) * len(train_and_validate))
        training_queries = train_and_validate[:split]
        validation_queries = train_and_validate[split:]
    elif validate_ratio == 0:
        training_queries = train_and_validate
        validation_queries = []
    else:
        raise ValueError()

    print(datetime.datetime.now())
    if old_model is None:
        encoder = build_encoder(schema, config, training_queries)
    else:
        encoder = old_model.encoder()

    if config["selfsupervised_factor"] > 0:
        if isinstance(schema, SQLSchema):
            value_picker = SQLRandomAttributeBuffer(schema)
            if query_generator is None:
                query_generator = QueryGeneratorChoice(sum(training_queries, []))
            card_rel_data_generator = CardinalityRelationDataGenerator(query_generator,
                                                                       encoder,
                                                                       [CardRelComplementarity(schema),
                                                                        CardRelGreaterMonotonicity(schema, value_picker),
                                                                        # CardRelOrMonotonicity(schema, value_picker),
                                                                        CardRelForeignKey(schema),
                                                                        CardRelInclusionExclusion(schema)],
                                                                       device)
        else:
            raise NotImplementedError()
    else:
        card_rel_data_generator = None

    if old_model is None:
        model = config['type'].from_config(encoder, config)
    else:
        model = old_model

    print("%d trainable parameters" % count_parameters(model))

    batch_size = int(config["batch_size"])
    model.to(device)

    print("Encoding queries")
    print(datetime.datetime.now())
    encoded_training_queries = encode(training_queries, model, device)
    encoded_validation_queries = encode(validation_queries, model, device)

    if card_rel_data_generator is not None:
        num_relations = batch_size
        if isinstance(model, BulkJGMPCardinalityModel) or isinstance(model, BulkMSCNCardinalityModel):
            num_relations = min(int(num_relations * np.mean([len(query) for query in training_queries])), 1000)
        card_rel_gen = (card_rel_data_generator, num_relations)
        selfsupervised_factor = config["selfsupervised_factor"]
    else:
        card_rel_gen = None
        selfsupervised_factor = 0

    train(model,
          encoded_training_queries,
          encoded_validation_queries,
          learning_rate=config["learning_rate"],
          epochs=config["epochs"],
          batch_size=batch_size,
          card_rel_gen=card_rel_gen,
          selfsupervised_factor=selfsupervised_factor)

    print()
    print("Training Set")
    test(model, encoded_training_queries)

    if len(encoded_validation_queries) > 0:
        print()
        print("Validation Set")
        test(model, encoded_validation_queries)

    means = []

    print()
    print()
    print("Test Sets")

    for name, queries in tests:
        print()
        print(name)
        encoded_test = encode(queries, model, device)
        means.append(test(model, encoded_test))

    if len(means) == 0:
        return 0, encoder, model

    return np.mean(means), encoder, model


def multi_benchmark(query_db: QueryDB,
                    benchmarks: List[Tuple[GraphlikeSchema, int, str, List[str]]], 
                    device: torch.device,
                    config: Optional[dict] = None
                    ) -> Dict[str, Any]:
    means = []
    for schema, estimator_id, train_and_validate, tests in benchmarks:
        mean, _, _ = load_train_test(query_db, schema, train_and_validate, tests, device, estimator_id, config=config)
        if np.isnan(mean):
            return {"status": hyperopt.STATUS_FAIL}
        means.append(mean)
    return {"loss": np.mean(means), "status": hyperopt.STATUS_OK}


def separate_train_test_experiment(schema: GraphlikeSchema,
                                   query_db: QueryDB,
                                   device: torch.device,
                                   model_descriptions: List[Tuple[str, Dict[str, Any], bool]],
                                   train_and_validate_name: str,
                                   test_names: List[Tuple[str, bool]],
                                   experiment_type: str,
                                   estimator_id: int):
    train_and_validate_queries = query_db.load_query_set(schema, train_and_validate_name, estimator_id, cardinality=True, only_with_true_cardinality=True)
    train_and_validate_queries = [[query] for query in train_and_validate_queries]

    tests = []
    setup_tests = []
    for test_name, runtimes in test_names:
        test_queries = query_db.load_query_set(schema, test_name, estimator_id, cardinality=True, only_with_true_cardinality=True)
        tests.append((test_name, runtimes, test_queries))
        setup_tests.append((test_name, test_queries))
    setups = []
    for name, config, semi_supervised in model_descriptions:
        setups.append(TrainingSetup(schema, name, train_and_validate_queries, None, setup_tests, config))
    experiment(schema, query_db, setups, tests, device, experiment_type=experiment_type)


def job_light_test(query_db: QueryDB,
                   device: torch.device,
                   configs: List[Dict[str, Any]],
                   repetitions: int = 10,
                   port: Optional[int] = None):
    schema = imdb_light_schema(port=port)
    # ["scale", "synthetic", "job-light"]
    increment_training_experiment(query_db, schema, "mscn_train", [("job-light", True)], configs, SubqueryMode.NONE, repetitions, device, 2, subset_sizes=[2**10, 2**11, 2**12, 2**13, 2**14, 2**15, 2**16])


def imdb_ceb_test(query_db: QueryDB,
                  device: torch.device,
                  configs: List[Dict[str, Any]],
                  subquery_mode: SubqueryMode,
                  repetitions: int = 10,
                  port: Optional[int] = None):
    schema = imdb_schema(port=port)
    increment_training_experiment(query_db, schema, "flowloss-imdb-unique", [("job", False)], configs, subquery_mode, repetitions, device, 2)


def flatten(l: list):
    return [item for sublist in l for item in sublist]


def load_splits(query_db: QueryDB,
                schema: GraphlikeSchema,
                query_set_name: str,
                grouped: bool,
                subquery_mode: SubqueryMode,
                repetitions: int,
                num_parts: int) -> Tuple[List[Tuple[List[List[GraphlikeQuery]], Optional[QueryGenerator], List[Tuple[str, List[List[GraphlikeQuery]]]]]], List[Tuple[str, bool, List[GraphlikeQuery]]]]:
    queries, _, _, group_cardinalities, _, _, query_names, group_ids = query_db.load_group_cardinalities(schema, query_set_name, 2, True)

    if grouped:
        groups = group_job(query_names)
    else:
        groups = [[query] for query in queries]

    split_groups = multi_split(repetitions, num_parts, groups)
    split_groups = [(flatten(train), flatten(test)) for train, test in split_groups]

    tests = [(query_set_name, True, queries)]
    subqueries = get_all_subqueries(queries, group_cardinalities, group_ids)
    tests.insert(0, (query_set_name + "_subqueries", False, sum([subqueries[query] for query in queries], [])))
    if subquery_mode == SubqueryMode.BEST:
        best_subqueries = get_best_subqueries(schema, queries, subqueries)
        tests.insert(0, (query_set_name + "_best_subqueries", False, sum([best_subqueries[query] for query in queries], [])))

    splits = []
    for train, test in split_groups:
        root_subqueries_test = [[query] for query in test]
        generator = QueryGeneratorChoice(sum([subqueries[query] for query in train], []))
        if subquery_mode == SubqueryMode.NONE:
            root_subqueries_train = [[query] for query in train]
            splits.append((root_subqueries_train, generator, [(query_set_name, root_subqueries_test)]))
        elif subquery_mode == SubqueryMode.BEST:
            subquery_test = [subqueries[query] for query in test]
            best_subquery_train = [best_subqueries[query] for query in train]
            best_subquery_test = [best_subqueries[query] for query in test]
            splits.append((best_subquery_train, generator, [(query_set_name, root_subqueries_test), (query_set_name + "_subqueries", subquery_test), (query_set_name + "_best_subqueries", best_subquery_test)]))
        elif subquery_mode == SubqueryMode.ALL:
            subquery_train = [subqueries[query] for query in train]
            subquery_test = [subqueries[query] for query in test]
            splits.append((subquery_train, generator, [(query_set_name, root_subqueries_test), (query_set_name + "_subqueries", subquery_test)]))

    return splits, tests


def get_all_subqueries(queries: List[GraphlikeQuery],
                       group_cardinalities: Dict[GraphlikeQuery, Dict[FrozenSet[QueryNode], int]],
                       group_ids: Dict[GraphlikeQuery, Dict[FrozenSet[QueryNode], int]]
                       ) -> Dict[GraphlikeQuery, List[GraphlikeQuery]]:
    subqueries = {}
    for query in queries:
        current_subqueries = []
        assert (isinstance(query, SQLQuery))
        for node_set in group_cardinalities[query]:
            group_query = query.build_subquery(node_set, group_cardinalities[query][node_set], id=group_ids[query][node_set])
            current_subqueries.append(group_query)
        subqueries[query] = current_subqueries
    return subqueries


def get_best_subqueries(schema: GraphlikeSchema,
                        queries: List[GraphlikeQuery],
                        subqueries: Dict[GraphlikeQuery, List[GraphlikeQuery]]
                        ) -> Dict[GraphlikeQuery, List[GraphlikeQuery]]:
    not_found_count = 0
    total = 0
    best_subqueries = {}
    if isinstance(schema, SQLSchema):
        pg_explain = PgExplain(True)
        cursor = schema.connection().cursor()
        for query in queries:
            table_subquery = {}
            for subquery in subqueries[query]:
                table_subquery[frozenset([node for node in subquery.nodes() if not node.virtual()])] = subquery
            cursor.execute("EXPLAIN (FORMAT JSON) " + query.sql(count=True))
            explain = cursor.fetchone()[0][0]
            table_sets = pg_explain.extract_table_sets(query, explain, index_scans=False)
            best_subqueries[query] = []
            for table_set in table_sets:
                total += 1
                if table_set in table_subquery:
                    best_subqueries[query].append(table_subquery[table_set])
                else:
                    not_found_count += 1
        cursor.close()
    else:
        raise NotImplementedError()
    print("Not found count: %d/%d" % (not_found_count, total))
    return best_subqueries


def crossvalidation_experiment(query_db: QueryDB,
                               schema: GraphlikeSchema,
                               query_set: str,
                               configs: List[Dict[str, Any]],
                               subquery_mode: SubqueryMode,
                               repetitions: int,
                               num_parts: int,
                               device: torch.device,
                               grouped: bool = False):
    splits, tests = load_splits(query_db, schema, query_set, grouped, subquery_mode, repetitions, num_parts)
    setups = build_setups(splits, schema, configs)
    experiment_name = "crossvalidation-%s-%s" % (query_set, subquery_mode.name)
    if grouped:
        experiment_name += "-grouped"
    experiment(schema, query_db, setups, tests, device, experiment_name)


def increment_training_experiment(query_db: QueryDB,
                                  schema: GraphlikeSchema,
                                  training_set: str,
                                  test_sets: List[Tuple[str, bool]],
                                  configs: List[Dict[str, Any]],
                                  subquery_mode: SubqueryMode,
                                  repetitions: int,
                                  device: torch.device,
                                  estimator_id: int = 2,
                                  subset_sizes: List[int] = [2**7, 2**8, 2**9, 2**10, 2**11, 2**12]):
    if subquery_mode == SubqueryMode.NONE:
        queries = query_db.load_query_set(schema, training_set, estimator_id, cardinality=True)
        train_subqueries = {query: [query] for query in queries}
    else:
        queries, _, _, group_cardinalities, _, _, _, group_ids = query_db.load_group_cardinalities(schema, training_set, estimator_id, True)
        train_subqueries = get_all_subqueries(queries, group_cardinalities, group_ids)
        if subquery_mode == SubqueryMode.BEST:
            best_subqueries = get_best_subqueries(schema, queries, train_subqueries)

    training_query_sets = incremental_subsets(repetitions, subset_sizes, queries)

    tests = []
    split_tests = []
    for test_set, runtime_tests in test_sets:
        test_queries, _, _, test_group_cardinalities, _, _, _, test_group_ids = query_db.load_group_cardinalities(schema, test_set, estimator_id, True)
        subqueries = get_all_subqueries(test_queries, test_group_cardinalities, test_group_ids)
        tests.append((test_set + "_subqueries", False, [subqueries[query] for query in test_queries]))
        tests.append((test_set, runtime_tests, [[tq] for tq in test_queries]))
        split_tests.append((test_set + "_subqueries", [subqueries[query] for query in test_queries]))
        split_tests.append((test_set, [[tq] for tq in test_queries]))

    tests.append((training_set + "_subqueries", False, [train_subqueries[query] for query in train_subqueries]))
    tests.append((training_set, False, [[query] for query in train_subqueries]))


    setups = []
    for query_set in training_query_sets:
        set_size = str(len(query_set[0][0]))
        for queries, test_queries in query_set:
            if subquery_mode == SubqueryMode.NONE:
                training_queries = [[query] for query in queries]
            elif subquery_mode == SubqueryMode.BEST:
                training_queries = [best_subqueries[query] for query in queries]
            elif subquery_mode == subquery_mode.ALL:
                training_queries = [train_subqueries[query] for query in queries]
            query_choices = sum([train_subqueries[query] for query in queries], [])
            additional_split_tests = []
            additional_split_tests.append((training_set + "_subqueries", [train_subqueries[query] for query in test_queries]))
            additional_split_tests.append((training_set, [[query] for query in test_queries]))
            for config in configs:
                config = config.copy()
                config["name"] = config["name"] + "_" + set_size
                generator = QueryGeneratorChoice(query_choices)
                setups.append(TrainingSetup(schema, training_queries, generator, split_tests + additional_split_tests, config, 0))

    experiment_name = "increment-%s-%s" % (training_set, subquery_mode.name)
    experiment(schema, query_db, setups, tests, device, experiment_name)


def workload_shift_experiment(query_db: QueryDB,
                              schema: SQLSchema,
                              config_path: str,
                              first_training_name: str,
                              second_training_name: str,
                              second_training_size: int,
                              new_max_samples: int,
                              device: torch.device):
    rand = random.Random(42)
    # extract original test sets from file
    card_est_filename = config_path + "/" + first_training_name + "_card_est.csv"
    all_query_ids = set()
    model_test_sets = {}
    with open(card_est_filename, "r") as f:
        lines = f.readlines()
        for line in lines:
            query_id, model_id, _ = line.split(",")
            query_id = int(query_id)
            model_id = int(model_id)
            if model_id not in model_test_sets:
                model_test_sets[model_id] = []
            model_test_sets[model_id].append(query_id)
            all_query_ids.add(query_id)
    # compute original training sets by subtracting test sets from all queries
    original_training_sets = {}
    for model_id, test_set in model_test_sets.items():
        original_training_sets[model_id] = all_query_ids.difference(test_set)

    first_queries, _, _, first_group_cardinalities, _, _, _, first_group_ids = query_db.load_group_cardinalities(schema, first_training_name, 2, True)
    first_all_subqueries = get_all_subqueries(first_queries, first_group_cardinalities, first_group_ids)
    first_best_subqueries = get_best_subqueries(schema, first_queries, first_all_subqueries)

    second_queries, _, _, second_group_cardinalities, _, _, _, second_group_ids = query_db.load_group_cardinalities(schema, second_training_name, 2, True)
    second_all_subqueries = get_all_subqueries(second_queries, second_group_cardinalities, second_group_ids)
    second_best_subqueries = get_best_subqueries(schema, second_queries, second_all_subqueries)

    tests = []
    tests.append((first_training_name + "_subqueries", False, [first_all_subqueries[query] for query in first_queries]))
    tests.append((first_training_name, False, [[tq] for tq in first_queries]))
    tests.append((second_training_name + "_subqueries", False, [second_all_subqueries[query] for query in second_queries]))
    tests.append((second_training_name, True, [[tq] for tq in second_queries]))

    setups = []
    for model_name in os.listdir(config_path):
        if model_name.endswith(".pt") :
            model_name = model_name[:-3]
            model_id = int(model_name)
            model_path = config_path + "/" + model_name
            assert(second_training_size <= len(second_queries))
            first_training_queries = [query for query in first_queries if query.id() in original_training_sets[model_id]]
            second_training_queries = rand.sample(second_queries, second_training_size)
            first_training_all_subqueries = sum([first_all_subqueries[query] for query in first_training_queries], [])
            second_training_all_suqueries = sum([second_all_subqueries[query] for query in second_training_queries], [])
            first_training_best_subqueries = [first_best_subqueries[query] for query in first_training_queries]
            second_training_best_subqueries = [second_best_subqueries[query] for query in second_training_queries]
            combined_training_queries = first_training_queries + second_training_queries
            pure_training_all_subqueries = second_training_all_suqueries
            combined_training_all_subqueries = first_training_all_subqueries + second_training_all_suqueries
            equal_training_all_subqueries = first_training_all_subqueries + second_training_all_suqueries * (len(first_training_all_subqueries) // len(second_training_all_suqueries))
            pure_training_best_subqueries = second_training_best_subqueries
            combined_training_best_subqueries = first_training_best_subqueries + second_training_best_subqueries
            equal_training_best_subqueries = first_training_best_subqueries + second_training_best_subqueries * (len(first_training_best_subqueries) // len(second_training_best_subqueries))

            first_test_queries = [query for query in first_queries if query.id() in model_test_sets[model_id]]
            second_test_queries = set(second_queries).difference(second_training_queries)
            first_test_subqueries = [first_all_subqueries[query] for query in first_test_queries]
            second_test_subqueries = [second_all_subqueries[query] for query in second_test_queries]
            setup_tests = [(first_training_name, [[q] for q in first_test_queries]),
                           (second_training_name, [[q] for q in second_test_queries]),
                           (first_training_name + "_subqueries", first_test_subqueries),
                           (second_training_name + "_subqueries", second_test_subqueries)]

            pure_query_generator = QueryGeneratorChoice(pure_training_all_subqueries)
            combined_query_generator = QueryGeneratorChoice(combined_training_all_subqueries)
            equal_query_generator = QueryGeneratorChoice(equal_training_all_subqueries)

            retrain_config = jgmp_config.copy()
            retrain_config["name"] += "-retrain"
            pure_tune_config = jgmp_config.copy()
            pure_tune_config["name"] += "-pure-tune"
            combined_tune_config = jgmp_config.copy()
            combined_tune_config["name"] += "-combined-tune"
            combined_tune_config["num_epochs"] = 100
            equal_tune_config = jgmp_config.copy()
            equal_tune_config["name"] += "-equal-tune"
            equal_tune_config["num_epochs"] = 50

            retrain_setup = TrainingSetup(schema, combined_training_best_subqueries, combined_query_generator, setup_tests, retrain_config, 0)
            setups.append(retrain_setup)
            pure_tune_estimator = load_learned_estimator_sql(schema, model_path, jgmp_config, device=device)
            pure_tune_model = pure_tune_estimator.cardinality_model()
            old_sampler = pure_tune_model.encoder().sampler()
            new_sampler = SQLSampler.build_sampler_fs(schema, new_max_samples, 100000, 1000, 1000, combined_training_queries, old_sampler=old_sampler)
            new_sampler_bitmap_size = new_sampler.bitmap_size()
            new_max_samples = new_sampler.bitmap_size()
            pure_tune_model.encoder().set_sampler(new_sampler)
            pure_tune_model.pad_sample_weights(new_sampler_bitmap_size)
            pure_tune_setup = TrainingSetup(schema, pure_training_best_subqueries, pure_query_generator, setup_tests, pure_tune_config, 0, old_model=pure_tune_model)
            setups.append(pure_tune_setup)
            combined_tune_estimator = load_learned_estimator_sql(schema, model_path, jgmp_config, device=device)
            combined_tune_model = combined_tune_estimator.cardinality_model()
            combined_tune_model.encoder().set_sampler(new_sampler)
            combined_tune_model.pad_sample_weights(new_sampler_bitmap_size)
            combined_tune_setup = TrainingSetup(schema, combined_training_best_subqueries, combined_query_generator, setup_tests, combined_tune_config, 0, old_model=combined_tune_model)
            setups.append(combined_tune_setup)
            equal_tune_estimator = load_learned_estimator_sql(schema, model_path, jgmp_config, device=device)
            equal_tune_model = equal_tune_estimator.cardinality_model()
            equal_tune_model.encoder().set_sampler(new_sampler)
            equal_tune_model.pad_sample_weights(new_sampler_bitmap_size)
            equal_tune_setup = TrainingSetup(schema, equal_training_best_subqueries, equal_query_generator, setup_tests, equal_tune_config, 0, old_model=equal_tune_model)
            setups.append(equal_tune_setup)

    experiment_name = "workload-shift-%s-%s" % (first_training_name, second_training_name)
    experiment(schema, query_db, setups, tests, device, experiment_name)


def build_setups(splits: List[Tuple[List[List[GraphlikeQuery]], Optional[QueryGenerator], List[Tuple[str, List[List[GraphlikeQuery]]]]]],
                 schema: GraphlikeSchema,
                 configs: List[Dict[str, Any]]
                 ) -> List[TrainingSetup]:
    setups = []
    for config in configs:
        for training_queries, generator, tests in splits:
            setups.append(TrainingSetup(schema, training_queries, generator, tests, config, 0))
    return setups


def job_light_only_test(query_db: QueryDB,
                        device: torch.device,
                        subquery_mode: SubqueryMode,
                        configs: List[Dict[str, Any]],
                        repetitions: int = 10,
                        num_parts: int = 5,
                        port: Optional[int] = None):
    schema = imdb_light_schema(port=port)
    crossvalidation_experiment(query_db, schema, "job-light", configs, subquery_mode, repetitions, num_parts, device)


def job_only_test(query_db: QueryDB,
                  device: torch.device,
                  subquery_mode: SubqueryMode,
                  configs: List[Dict[str, Any]],
                  repetitions: int = 10,
                  num_parts: int = 5,
                  grouped: bool = False,
                  port: Optional[int] = None):
    schema = imdb_schema(port=port)
    crossvalidation_experiment(query_db, schema, "job", configs, subquery_mode, repetitions, num_parts, device, grouped=grouped)


def stats_only_test(query_db: QueryDB,
                    device: torch.device,
                    subquery_mode: SubqueryMode,
                    configs: List[Dict[str, Any]],
                    repetitions: int = 10,
                    num_parts: int = 5,
                    port: Optional[int] = None):
    schema = stats_schema(port=port)
    crossvalidation_experiment(query_db, schema, "stats-ceb", configs, subquery_mode, repetitions, num_parts, device)


def get_baseline_runtimes_imdb(query_db: QueryDB, test_name: str, port: Optional[int] = None):
    schema = imdb_schema(port=port)
    queries = query_db.load_query_set(schema, test_name, 2)
    plan_engine = StubbornPlanEngine(schema, True, count=True, timeout= 3 * 60 * 60)
    baseline_runtimes(test_name, queries, "results/cardinality_estimation/%s_" % test_name, plan_engine)


def get_competitors_job_light(query_db: QueryDB, disable_nested_loop: bool = False, port: Optional[int] = None):
    schema = imdb_light_schema(port=port)
    get_competitors(schema,
                    query_db,
                    "job-light",
                    [("true", True), ("true", False), ("bayescard", False), ("deepdb", False), ("flat", False), ("neurocard", False)],
                    disable_nested_loop=disable_nested_loop)


def get_competitors_job(query_db: QueryDB, disable_nested_loop: bool = False, port: Optional[int] = None):
    schema = imdb_schema(port=port)
    get_competitors(schema,
                    query_db,
                    "job",
                    [("true", True)],
                    disable_nested_loop=disable_nested_loop)


def get_competitors_stats(query_db: QueryDB, disable_nested_loop: bool = False, port: Optional[int] = None):
    schema = stats_schema(port=port)
    get_competitors(schema,
                    query_db,
                    "stats-ceb",
                    [("true", True), ("true", False), ("bayescard", False), ("deepdb", False), ("flat", False), ("neurocard", False)],
                    disable_nested_loop=disable_nested_loop)


def debug_fs(query_db: QueryDB,
             device: torch.device):
    schema = SQLSchema.sql_schema_from_connection("imdb", leafs=["comp_cast_type", "company_type", "info_type", "kind_type", "link_type"])
    increment_training_experiment(query_db, schema, "flowloss-imdb-unique", [("job", False)], [jgmp_config], SubqueryMode.BEST, 1, device, 2, subset_sizes=[64])


def load_and_get_imdb_runtimes(query_db: QueryDB,
                               experiment_path: str,
                               test_name: str,
                               configs: List[Dict[str, Any]],
                               num_queries: Optional[int] = None,
                               num_models: Optional[int] = None):
    schema = imdb_schema()
    load_and_get_runtimes(schema, query_db, experiment_path, test_name, 2, configs, num_queries=num_queries, num_models=num_models)


def workload_shift_experiment_imdb(query_db: QueryDB,
                                   config_path: str,
                                   device: torch.device):
    schema = imdb_schema()
    workload_shift_experiment(query_db, schema, config_path, "flowloss-imdb-unique", "job", 56, 24, device)

