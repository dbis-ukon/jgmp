import datetime
import json
import math
import os
import shutil
from random import Random
from typing import List, Tuple, Dict, Any, Optional

import torch

from cardinality_configs import default_configs, semisupervised_experiment_configs, jgmp_config
from cardinality_estimator.learned_cardinality_estimator import LearnedCardinalityEstimator
from cardinality_estimator.postgres_cardinality_estimator import PostgresCardinalityEstimator
from cardinality_estimator.prophetic_alias_cardinality_estimator import PropheticAliasCardinalityEstimator
from cardinality_injection import parse_get_runtime_sql, extract_parse_subplans, get_data_directory, create_subplan_order_files, save_estimations, execute_query
from cardinality_test import train_test, baseline_runtimes, SubqueryMode
from plan_execution.pg_explain import PgExplain
from plan_execution.stubborn_plan_engine import StubbornPlanEngine
from query.query_generator_choice import QueryGeneratorChoice
from query.sql.sql_query import SQLQuery
from query.symmetry.eliminate_cross_product import detect_cross_product
from schema.schemas import dsb_schema
from schema.sql.sql_schema import SQLSchema
from training_test_split import multi_split


def get_queries(directory: str, queries_per_template: Optional[int] = None) -> List[Tuple[str, str]]:
    if queries_per_template is not None:
        name_options = set("query_%d.sql" % i for i in range(queries_per_template))
    queries = []
    for path, subdirs, files in os.walk(directory):
        for name in files:
            if name.endswith(".sql") and (queries_per_template is None or name in name_options):
                file_path = os.path.join(path, name)
                file = open(file_path, "r")
                query = file.read()
                file.close()
                queries.append((file_path[len(directory) + 1:-4], query))
    return queries


def train_test_split(queries: List[Tuple[str, str]]) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    query_copy = queries.copy()
    rand = Random(42)
    rand.shuffle(query_copy)
    return query_copy[:int(len(query_copy) * 0.8)], query_copy[int(len(query_copy) * 0.8):]


def get_training_set(schema: SQLSchema, queries: List[Tuple[str, str]], best: bool = True, only_inner: bool = True) -> List[Tuple[str, List[SQLQuery]]]:
    cursor = schema.connection().cursor()
    parsed = []
    for query_name, query in queries:
        if best:
            cursor.execute("EXPLAIN (FORMAT JSON) " + query)
            print(query_name)
            explain = cursor.fetchone()
            subplans = PgExplain.build_subplan_queries(schema, explain[0])
        else:
            data_directory = get_data_directory(schema)
            single_table, multi_table, _ = extract_parse_subplans(schema, query, data_directory, only_inner=only_inner)
            subplans = [subplan for subplan in single_table + multi_table if subplan is not None and not detect_cross_product(subplan)]
        parsed.append((query_name, subplans))
        for subplan in parsed[-1][1]:
            print(subplan.sql())
        print()
    cursor.close()
    return parsed


def get_table_instances(schema: SQLSchema, query: str) -> Dict[str, int]:
    cursor = schema.connection().cursor()
    cursor.execute("EXPLAIN (FORMAT JSON) " + query)
    explain = cursor.fetchone()
    cursor.close()
    subplan_queries = PgExplain.build_subplan_queries(schema, explain[0])
    table_instances = {}
    for subplan_query in subplan_queries:
        nodes = subplan_query.nodes()
        if len(nodes) == 1:
            table_instance = nodes[0]
            table_instances[subplan_query.alias(table_instance)] = table_instance


def extract_applicable_subplan_queries(schema: SQLSchema, query: str) -> List[Optional[SQLQuery]]:
    table_instances = get_table_instances(schema, query)


def dsb_save_cardinalities(query_directory: str, queries_per_template: Optional[int] = None):
    schema = dsb_schema()
    queries = get_queries(query_directory, queries_per_template=queries_per_template)
    training_set = get_training_set(schema, queries, best=False)
    cardinality_file = query_directory + "/cardinalities.json"
    load_cardinalities(schema, cardinality_file, training_set, max_subplans=1000)


def load_cardinalities(schema: SQLSchema, cardinality_file: str, queries: List[Tuple[str, List[SQLQuery]]], max_subplans: Optional[int] = None) -> List[Tuple[str, List[SQLQuery]]]:
    # load json file
    if os.path.exists(cardinality_file):
        file = open(cardinality_file, "r")
        cardinality_json = json.loads(file.read())
        file.close()
    else:
        cardinality_json = {}
    cursor = schema.connection().cursor()
    changed = False
    for i, (query_name, subplan_queries) in enumerate(queries):
        print("Query %d/%d: %s - %s" % (i + 1, len(queries), query_name, str(datetime.datetime.now())))
        if query_name not in cardinality_json:
            cardinality_json[query_name] = []
        query_json = cardinality_json[query_name]
        known_cardinalities = {}
        if max_subplans is not None and len(subplan_queries) > max_subplans:
            rand = Random(query_name)
            rand.shuffle(subplan_queries)
            del subplan_queries[max_subplans:]
        for subplan_query_json in query_json:
            node_set = frozenset([(alias, h) for alias, h in subplan_query_json["nodes"]])
            known_cardinalities[node_set] = subplan_query_json["cardinality"]
        for subplan_query in subplan_queries:
            subplan_query_node_set = frozenset([(subplan_query.alias(node), node.hash()) for node in subplan_query.nodes() if not node.virtual()])
            if subplan_query_node_set in known_cardinalities:
               subplan_query.cardinality_estimates()["true"] = known_cardinalities[subplan_query_node_set]
            else:
                cursor.execute(subplan_query.sql(count=True))
                cardinality = cursor.fetchone()[0]
                subplan_query.cardinality_estimates()["true"] = cardinality
                nodes = [[subplan_query.alias(node), node.hash()] for node in subplan_query.nodes() if not node.virtual()]
                query_json.append({"nodes": nodes, "cardinality": cardinality})
                changed = True
    cursor.close()
    if changed:
        file = open(cardinality_file, "w")
        file.write(json.dumps(cardinality_json))
        file.close()
    return queries

def build_alias_prophet(schema: SQLSchema, query_name: str, query: str, cardinality_path: str) -> PropheticAliasCardinalityEstimator:
    parsed = get_training_set(schema, [(query_name, query)], best=False)
    subplan_queries = load_cardinalities(schema, cardinality_path, parsed)[0][1]
    cardinalities = {frozenset([(subplan_query.alias(node), node.hash()) for node in subplan_query.nodes() if not node.virtual()]): subplan_query.cardinality_estimates()["true"] for subplan_query in subplan_queries}
    estimator = PropheticAliasCardinalityEstimator(schema, cardinalities, throw_key_error=False)
    return estimator


def postgres_estimation_runtimes(schema: SQLSchema, test_name: str, queries: List[Tuple[str, str]], result_path: str, plan_engine: StubbornPlanEngine, cardinality_path: str):
    runtime_path = result_path + test_name + "_pg_est_runtimes.csv"
    pg_estimate_path = result_path + "pg_est/"
    os.makedirs(pg_estimate_path, exist_ok=True)
    data_directory = get_data_directory(schema)
    file = open(runtime_path, "w")
    estimator = PostgresCardinalityEstimator(False, schema)
    for query_name, query in queries:
        _, runtime = parse_get_runtime_sql(schema, query, estimator, plan_engine)
        if runtime is None:
            output = "%s,%d,%f,None" % (query_name, 0, 0)
        else:
            output = "%s,%d,%f,%f" % (query_name, 0, 0, runtime)
        print(output)
        file.write(output + "\n")
        copy_estimation_files(data_directory, pg_estimate_path, query_name)
    file.close()


def postgres_injection_runtimes(schema: SQLSchema, test_name: str, queries: List[Tuple[str, str]], result_path: str, plan_engine: StubbornPlanEngine):
    data_directory = get_data_directory(schema)
    runtime_path = result_path + test_name + "_pg_injection_runtimes.csv"
    single_table_file_name = "single_table_estimations.txt"
    multi_table_file_name = "multi_table_estimations.txt"
    pg_injection_path = result_path + "pg_inj/"
    os.makedirs(pg_injection_path, exist_ok=True)
    runtime_file = open(runtime_path, "w")
    for query_name, query in queries:
        single_table_file, multi_table_file = create_subplan_order_files(schema, query, data_directory)
        for file, save_path in [(single_table_file, single_table_file_name), (multi_table_file, multi_table_file_name)]:
            content = open(file, "r").read()
            cardinalities = [int(line.split(":")[1].split(",")[0]) for line in content.split("\n") if '"rows":' == line[:7]]
            save_estimations(cardinalities, data_directory + save_path)
        runtime = execute_query(single_table_file_name, multi_table_file_name, query, plan_engine, single_table=True, print_explain=False, analyze=True)
        if runtime is None:
            output = "%s,%d,%f,None" % (query_name, 0, 0)
        else:
            output = "%s,%d,%f,%f" % (query_name, 0, 0, runtime)
        print(output)
        runtime_file.write(output + "\n")
        copy_estimation_files(data_directory, pg_injection_path, query_name)
    runtime_file.close()


def truecard_runtimes(schema: SQLSchema, test_name: str, queries: List[Tuple[str, str]], result_path: str, plan_engine: StubbornPlanEngine, cardinality_path: str):
    runtime_path = result_path + test_name + "_truecard_runtimes.csv"
    true_card_path = result_path + "true_card/"
    os.makedirs(true_card_path, exist_ok=True)
    file = open(runtime_path, "w")
    data_directory = get_data_directory(schema)
    for query_name, query in queries:
        estimator = build_alias_prophet(schema, query_name, query, cardinality_path)
        _, runtime = parse_get_runtime_sql(schema, query, estimator, plan_engine)
        if runtime is None:
            output = "%s,%d,%f,None" % (query_name, 0, 0)
        else:
            output = "%s,%d,%f,%f" % (query_name, 0, 0, runtime)
        print(output)
        file.write(output + "\n")
        copy_estimation_files(data_directory, true_card_path, query_name)
    file.close()


def copy_estimation_files(data_directory: str, result_path: str, query_name: str):
    shutil.copyfile(data_directory + "single_table_estimations.txt", result_path + query_name.replace("/", "_") + "_single.txt")
    shutil.copyfile(data_directory + "multi_table_estimations.txt", result_path + query_name.replace("/", "_") + "_multi.txt")
    shutil.copyfile(data_directory + "single_table_subplan_order.txt", result_path + query_name.replace("/", "_") + "_so_single.txt")
    shutil.copyfile(data_directory + "multi_table_subplan_order.txt", result_path + query_name.replace("/", "_") + "_so_multi.txt")


def dsb_baselines(query_directory: str,
                  queries_per_template: Optional[int] = None,
                  timeout: Optional[int] = 3 * 60 * 60,
                  port: int = 5432):
    date_string = datetime.datetime.now().isoformat(timespec="seconds")
    dir_path = os.path.dirname(os.path.realpath(__file__))
    result_path = dir_path + "/" + "results/dsb/experiment_" + date_string + "/"
    os.makedirs(result_path, exist_ok=True)
    schema = dsb_schema(port=port)

    queries = get_queries(query_directory, queries_per_template=queries_per_template)

    cardinality_path = query_directory + "/cardinalities.json"

    plan_engine = StubbornPlanEngine(schema, False, timeout=timeout)

    # postgres_estimation_runtimes(schema, "dsb", queries, result_path, plan_engine, cardinality_path)
    # postgres_injection_runtimes(schema, "dsb", queries, result_path, plan_engine)
    truecard_runtimes(schema, "dsb", queries, result_path, plan_engine, cardinality_path)
    baseline_runtimes("dsb", queries, result_path, plan_engine)


def dsb_test(query_directory: str,
             configs: List[Dict[str, Any]],
             subquery_mode: SubqueryMode,
             queries_per_template: Optional[int] = None,
             timeout: Optional[int] = 3 * 60 * 60,
             repetitions: int = 1,
             num_parts: int = 5,
             port: int = 5432):
    if subquery_mode == SubqueryMode.NONE:
        raise NotImplementedError
    date_string = datetime.datetime.now().isoformat(timespec="seconds")
    dir_path = os.path.dirname(os.path.realpath(__file__))
    result_path = dir_path + "/" + "results/dsb/experiment_" + date_string + "/"
    os.makedirs(result_path, exist_ok=True)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    schema = dsb_schema(port=port)

    queries = get_queries(query_directory, queries_per_template=queries_per_template)
    sql_dict = {query_name: query for query_name, query in queries}

    splits = multi_split(repetitions, num_parts, [query_name for query_name, _ in queries])

    cardinality_path = query_directory + "/cardinalities.json"
    only_inner = not subquery_mode == SubqueryMode.BEST
    all_subplan_queries = get_training_set(schema, queries, best=False, only_inner=only_inner)
    all_subplan_queries = load_cardinalities(schema, cardinality_path, all_subplan_queries)
    all_subplans_dict = {query_name: subplan_queries for query_name, subplan_queries in all_subplan_queries}

    if subquery_mode == SubqueryMode.BEST:
        best_subplan_queries = get_training_set(schema, queries, best=True)
        best_subplan_queries = load_cardinalities(schema, cardinality_path, best_subplan_queries)
        best_subplans_dict = {query_name: subplan_queries for query_name, subplan_queries in best_subplan_queries}

    plan_engine = StubbornPlanEngine(schema, False, timeout=timeout)

    # postgres_estimation_runtimes(schema, "dsb", queries, result_path, plan_engine, cardinality_path)
    # postgres_injection_runtimes(schema, "dsb", queries, result_path, plan_engine)
    # truecard_runtimes(schema, "dsb", queries, result_path, plan_engine, cardinality_path)
    baseline_runtimes("dsb", queries, result_path, plan_engine)
    print()

    data_directory = get_data_directory(schema)
    for model_no, (training_queries, test_queries) in enumerate(splits):
        if subquery_mode == SubqueryMode.BEST:
            training_set_pure = [best_subplans_dict[query_name] for query_name in training_queries]
            query_generator = QueryGeneratorChoice(sum([all_subplans_dict[query_name] for query_name in training_queries], []))
        elif subquery_mode == SubqueryMode.ALL:
            training_set_pure = [all_subplans_dict[query_name] for query_name in training_queries]
            query_generator = None
        else:
            raise NotImplementedError
        test_queries = [(query_name, sql_dict[query_name]) for query_name in test_queries]
        for config in configs:
            _, _, model = train_test(schema, training_set_pure, [], device, config, validate_ratio=0, query_generator=query_generator)
            estimator = LearnedCardinalityEstimator(model, device)
            config_path = result_path + config["name"] + "/"
            os.makedirs(config_path, exist_ok=True)
            estimator.save(config_path + str(model_no))
            cardinality_path = config_path + str(model_no) + "_cardinalities/"
            os.makedirs(cardinality_path, exist_ok=True)
            setup_file = config_path + "dsb_runtimes.csv"
            file = open(setup_file, "a+")
            for query_name, query in test_queries:
                estimation_time, runtime = parse_get_runtime_sql(schema, query, estimator, plan_engine)
                if runtime is None:
                    output = "%s,%d,%f,None" % (query_name, model_no, estimation_time)
                else:
                    output = "%s,%d,%f,%f" % (query_name, model_no, estimation_time, runtime)
                print(output)
                file.write(output + "\n")
                copy_estimation_files(data_directory, cardinality_path, query_name)
            file.close()



# dsb_save_cardinalities("/home/silvan/code/jgmp-revision/data/dsb", queries_per_template=10)
# configs = default_configs()
# configs = semisupervised_experiment_configs(jgmp_config)
# dsb_test("data/dsb", configs, subquery_mode=SubqueryMode.BEST, queries_per_template=10)

