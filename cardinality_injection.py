import json
import os
import re
import time
from enum import Enum
from typing import Tuple, Optional, FrozenSet, List, Dict, Union
from cardinality_estimator.cardinality_estimator import CardinalityEstimator
from cardinality_estimator.learned_cardinality_estimator import LearnedCardinalityEstimator
from plan_execution.pg_explain import PgExplain
from plan_execution.stubborn_plan_engine import StubbornPlanEngine
from query.predicate import ArbitraryPredicate
from query.sql.sql_query import SQLQuery
from query.sql.sql_table_instance import SQLTableInstance
from schema.sql.sql_schema import SQLSchema


# join type enum
class JoinType(Enum):
    INNER = 1
    LEFT = 2
    FULL = 3
    SEMI = 4
    ANTI = 5


# @profile(filename="profile.pstat")
def setup_cardinality_injection(schema: SQLSchema, query: SQLQuery, estimator: CardinalityEstimator, single_table: bool = True) -> Tuple[str, str, int]:
    data_directory = get_data_directory(schema)
    single_table_queries, multi_table_queries = build_subplan_queries(schema, data_directory, query)
    if not single_table:
        single_table_queries = []

    return estimate_and_save(single_table_queries, multi_table_queries, estimator, data_directory)


def get_data_directory(schema: SQLSchema) -> str:
    cursor = schema.connection().cursor()
    cursor.execute("SELECT setting FROM pg_settings WHERE name = 'data_directory';")
    data_directory = cursor.fetchone()[0] + "/"
    cursor.close()
    return data_directory


def estimate_and_save(single_table_queries: List[SQLQuery],
                      multi_table_queries: List[SQLQuery],
                      estimator: CardinalityEstimator,
                      data_directory: str,
                      join_infos: Optional[List[Tuple[JoinType, Tuple[bool, int], Tuple[bool, int]]]] = None) -> Tuple[str, str, int]:
    all_queries = single_table_queries + multi_table_queries
    if isinstance(estimator, LearnedCardinalityEstimator):
        estimator.encoder().reset()
    start = time.time()
    estimations = estimator.bulk_estimate([all_queries])
    estimation_time = time.time() - start
    if len(single_table_queries) > 0:
        single_table_estimations = estimations[:len(single_table_queries)]
        multi_table_estimations = estimations[len(single_table_queries):]
    else:
        single_table_estimations = []
        multi_table_estimations = estimations
    if join_infos is not None:
        assert(len(multi_table_estimations) == len(join_infos))
        for i in range(len(multi_table_estimations)):
            estimation = multi_table_estimations[i]
            join_info = join_infos[i]
            if estimation == -1 or join_info is None:
                continue
            join_type, (outer_t, outer_id), (inner_t, inner_id) = join_info
            if join_type == JoinType.INNER:
                continue

            if outer_t:
                outer_rows = multi_table_estimations[outer_id]
            else:
                outer_rows = single_table_estimations[outer_id]

            if inner_t:
                inner_rows = multi_table_estimations[inner_id]
            else:
                inner_rows = single_table_estimations[inner_id]

            if outer_rows == -1 or inner_rows == -1:
                estimation = -1
            # analogous to PostgreSQL's calc_joinrel_size_estimate
            elif join_type == JoinType.LEFT:
                if estimation < outer_rows:
                    estimation = outer_rows
            elif join_type == JoinType.FULL:
                max_rows = max(outer_rows, inner_rows)
                if estimation < max_rows:
                    estimation = max_rows
            elif join_type == JoinType.SEMI:
                estimation = -1
            elif join_type == JoinType.ANTI:
                estimation = outer_rows - estimation
                if estimation < 0:
                    estimation = 0
            else:
                raise NotImplementedError
            multi_table_estimations[i] = estimation
    single_table_file_name = "single_table_estimations.txt"
    multi_table_file_name = "multi_table_estimations.txt"
    if len(single_table_queries) > 0:
        save_estimations(single_table_estimations, data_directory + single_table_file_name)
    save_estimations(multi_table_estimations, data_directory + multi_table_file_name)
    return single_table_file_name, multi_table_file_name, estimation_time



def save_estimations(estimations: List[float], file_path: str):
    file = open(file_path, 'w')
    for estimation in estimations:
        file.write("%f\n" % estimation)
    file.close()


def get_runtime_sql(schema: SQLSchema,
                    query: SQLQuery,
                    estimator: CardinalityEstimator,
                    plan_engine: StubbornPlanEngine,
                    single_table: bool = True,
                    print_explain: bool = False,
                    analyze: bool = True
                    ) -> Tuple[float, Optional[float]]:
    single_table_file_name, multi_table_file_name, estimation_time = setup_cardinality_injection(schema, query, estimator, single_table=single_table)
    runtime = execute_query(single_table_file_name, multi_table_file_name, query, plan_engine, single_table=single_table, print_explain=print_explain, analyze=analyze)
    return estimation_time, runtime


def execute_query(single_table_file_name: str,
                  multi_table_file_name: str,
                  query: Union[SQLQuery, str],
                  plan_engine: StubbornPlanEngine,
                  single_table: bool = True,
                  print_explain: bool = False,
                  analyze: bool = True
                  ) -> Optional[float]:
    settings = ["SET ml_joinest_enabled = TRUE;",
                "SET query_no = 0;",
                "SET join_est_no = 0;",
                "SET ml_cardest_fname = '%s';" % single_table_file_name,
                "SET ml_joinest_fname = '%s';" % multi_table_file_name]

    if single_table:
        settings.insert(0, "SET ml_cardest_enabled = TRUE;")

    execution_result = plan_engine.execute_with_settings(query, None, settings, print_explain=print_explain, analyze=analyze)
    if execution_result is not None:
        _, runtime, _, _ = execution_result
        runtime = runtime / 1000
    else:
        runtime = None

    return runtime


def build_subplan_queries(schema: SQLSchema, data_directory: str, query: SQLQuery) -> Tuple[List[SQLQuery], List[SQLQuery]]:
    cursor = schema.connection().cursor()
    single_table_file = data_directory + "single_table_subplan_order.txt"
    multi_table_file = data_directory + "multi_table_subplan_order.txt"

    if os.path.exists(single_table_file):
        os.remove(single_table_file)

    if os.path.exists(multi_table_file):
        os.remove(multi_table_file)

    cursor.execute("SET print_sub_queries = TRUE;")
    cursor.execute("SET print_single_tbl_queries = TRUE;")

    cursor.execute("SET query_no = 0;")
    cursor.execute("SET join_est_no= 0;")
    cursor.execute("EXPLAIN " + query.sql())

    cursor.execute("SET print_sub_queries = FALSE;")
    cursor.execute("SET print_single_tbl_queries = FALSE;")

    cursor.close()

    single_table_subplan_queries = extract_subplan_queries(query, single_table_file)
    multi_table_subplan_queries = extract_subplan_queries(query, multi_table_file)

    return single_table_subplan_queries, multi_table_subplan_queries


def extract_subplan_queries(query: SQLQuery, file_path: str) -> List[SQLQuery]:
    subplan_order = extract_subplan_order(query, file_path)
    subplan_queries = []
    for subplan in subplan_order:
        subplan_queries.append(query.build_subquery(subplan, -1))
    return subplan_queries


def extract_subplan_order(query: SQLQuery, file_path: str) -> List[FrozenSet[SQLTableInstance]]:
    if not os.path.exists(file_path):
        return []
    subplan_order = []
    aliases = query.aliases()
    aliases_reversed = {}
    for predicatable in aliases:
        aliases_reversed[aliases[predicatable]] = predicatable
    file = open(file_path, "r")
    for line in file:
        # "table_aliases": ["store_sales", "store"],
        if line.startswith("\"table_aliases\""):
            list_string = line.split(":")[1].strip()[:-1]
            plan_aliases = json.loads(list_string)
            subplan = []
            for alias in plan_aliases:
                subplan.append(aliases_reversed[alias])
            subplan_order.append(frozenset(subplan))
        # "table_alias": "store_sales",
        elif line.startswith("\"table_alias\""):
            alias = line.split(":")[1].strip()[1:-2]
            subplan_order.append(frozenset([aliases_reversed[alias]]))
    file.close()
    return subplan_order


def parse_get_runtime_sql(schema: SQLSchema,
                          query: str,
                          estimator: CardinalityEstimator,
                          plan_engine: StubbornPlanEngine,
                          single_table: bool = True,
                          print_explain: bool = False,
                          analyze: bool = True
                          ) -> Tuple[float, Optional[float]]:
    single_table_file_name, multi_table_file_name, estimation_time = parse_setup_cardinality_injection(schema, query, estimator, single_table=single_table)
    runtime = execute_query(single_table_file_name, multi_table_file_name, query, plan_engine, single_table=single_table, print_explain=print_explain, analyze=analyze)
    return estimation_time, runtime


def parse_setup_cardinality_injection(schema: SQLSchema, query: str, estimator: CardinalityEstimator, single_table: bool = True) -> Tuple[str, str, int]:
    data_directory = get_data_directory(schema)
    single_table_queries, multi_table_queries, join_infos = extract_parse_subplans(schema, query, data_directory)
    if not single_table:
        single_table_queries = []

    return estimate_and_save(single_table_queries, multi_table_queries, estimator, data_directory, join_infos=join_infos)


def create_subplan_order_files(schema: SQLSchema, query: str, data_directory: str) -> Tuple[str, str]:
    cursor = schema.connection().cursor()

    single_table_file = data_directory + "single_table_subplan_order.txt"
    multi_table_file = data_directory + "multi_table_subplan_order.txt"

    if os.path.exists(single_table_file):
        os.remove(single_table_file)

    if os.path.exists(multi_table_file):
        os.remove(multi_table_file)

    cursor.execute("SET print_sub_queries = TRUE;")
    cursor.execute("SET print_single_tbl_queries = TRUE;")

    cursor.execute("SET query_no = 0;")
    cursor.execute("SET join_est_no = 0;")
    cursor.execute("EXPLAIN " + query)

    cursor.execute("SET print_sub_queries = FALSE;")
    cursor.execute("SET print_single_tbl_queries = FALSE;")

    cursor.close()
    return single_table_file, multi_table_file


def extract_parse_subplans(schema: SQLSchema, query: str, data_directory: str, only_inner: bool = False) -> Tuple[List[Optional[SQLQuery]], List[Optional[SQLQuery]], List[Optional[Tuple[JoinType, Tuple[bool, int], Tuple[bool, int]]]]]:
    single_table_file, multi_table_file = create_subplan_order_files(schema, query, data_directory)

    single_table_text = "[%s]" % open(single_table_file, "r").read()[:-2]
    single_table_json = json.loads(single_table_text)
    multi_table_text = "[%s]" % open(multi_table_file, "r").read()[:-2]
    multi_table_json = json.loads(multi_table_text)

    id_to_index = {}
    subplans = {}
    single_table_sublan_queries = []
    for subplan in single_table_json:
        alias = subplan["table_alias"]
        subplan_id = subplan["id"]
        if subplan["table"] == "(null)":
            subplan_query = None
            cardinality = -1
        else:
            predicate_strings = subplan["predicates"]
            if any(["unsupported expr" in predicate_string for predicate_string in predicate_strings]):
                subplan_query = None
                cardinality = -1
            else:
                table_name = subplan["table"]
                table = schema.node(table_name)
                arbitrary_predicates = [[ArbitraryPredicate("(%s)" % predicate_string)] for predicate_string in predicate_strings]
                predicates = PgExplain.parse_arbitrary_predicates(table, alias, arbitrary_predicates, injection_mode=True)
                table_instance = SQLTableInstance.build(schema, table, predicates, alias=alias)
                subplan_query = SQLQuery([table_instance], [])
                subplan_query.aliases()[table_instance] = alias
                cardinality = subplan["rows"]
        id_to_index[subplan_id] = (False, len(single_table_sublan_queries))
        single_table_sublan_queries.append(subplan_query)
        subplans[subplan_id] = (subplan_query, [], cardinality, None)

    multi_table_sublan_queries = []
    join_infos = []
    for subplan in multi_table_json:
        subplan_query, pushed_down_predicates, cardinality, join_info = build_join_subplan_query(schema, subplan, subplans, only_inner=only_inner)
        id_to_index[subplan["id"]] = (True, len(multi_table_sublan_queries))
        if len(pushed_down_predicates) > 0:
            multi_table_sublan_queries.append(None)
        else:
            multi_table_sublan_queries.append(subplan_query)
        if join_info is not None:
            join_info = (join_info[0], id_to_index[join_info[1]], id_to_index[join_info[2]])
        join_infos.append(join_info)
        subplans[subplan["id"]] = (subplan_query, pushed_down_predicates, cardinality, join_info)

    # percentage of non-parsable (at least for now) subplans for the curios researcher
    # print(len([None for sq in single_table_sublan_queries + multi_table_sublan_queries if sq is None]) / len(single_table_sublan_queries + multi_table_sublan_queries))

    return single_table_sublan_queries, multi_table_sublan_queries, join_infos


def build_join_subplan_query(schema: SQLSchema, subplan: dict, subplans: Dict[int, Tuple[Optional[SQLQuery], List[str], int, Optional[Tuple[JoinType, int, int]]]], only_inner: bool = False) -> Tuple[Optional[SQLQuery], List[str], int, Tuple[JoinType, Tuple[bool, int], Tuple[bool, int]]]:
    cardinality = subplan["rows"]
    if subplan["join_type"] == "Inner":
        join_type = JoinType.INNER
    elif only_inner:
        return None, [], cardinality, None
    elif subplan["join_type"] == "Left":
        join_type = JoinType.LEFT
    elif subplan["join_type"] == "Full":
        join_type = JoinType.FULL
    elif subplan["join_type"] == "Semi":
        join_type = JoinType.SEMI
    elif subplan["join_type"] == "Anti":
        join_type = JoinType.ANTI
    else:
        raise Exception("Unknown join type: %s" % subplan["join_type"])
    child_queries = []
    predicates = []
    child_ids = []
    for child in subplan["children"]:
        if child["id"] == -1:
            return None, [], cardinality, None
        child_query, child_predicates, child_cardinality, child_join_type = subplans[child["id"]]
        if child_query is None or child_cardinality != child["rows"] or (child_join_type is not None and child_join_type[0] != JoinType.INNER):
            return None, [], cardinality, None
        child_queries.append(child_query)
        predicates += child_predicates
        child_ids.append(child["id"])
    assert(len(child_queries) == 2)
    first_aliases = {v: k for k, v in child_queries[0].aliases().items() if not k.virtual()}
    second_aliases = {v: k for k, v in child_queries[1].aliases().items() if not k.virtual()}
    equivalence_connections = set()
    predicates += subplan["join_clauses"]
    pushed_down_predicates = []
    for join_condition in predicates:
        # check if join condition string is of the form "table1.column = table2.column"
        parts = join_condition.split()
        if len(parts) != 3 or parts[1] != "=":
            return None, [], cardinality, None
        try:
            left_alias, left_column = parts[0].split(".")
            right_alias, right_column = parts[2].split(".")
        except:
            return None, [], cardinality, None
        if left_alias in first_aliases and right_alias in second_aliases:
            first_alias = left_alias
            first_column = left_column
            second_alias = right_alias
            second_column = right_column
        elif left_alias in second_aliases and right_alias in first_aliases:
            first_alias = right_alias
            first_column = right_column
            second_alias = left_alias
            second_column = left_column
        else:
            pushed_down_predicates.append(join_condition)
            continue
        first_table_instance = first_aliases[first_alias]
        first_table = first_table_instance.table()
        first_attribute = first_table.attribute(first_column)
        second_table_instance = second_aliases[second_alias]
        second_table = second_table_instance.table()
        second_attribute = second_table.attribute(second_column)
        if not first_attribute in schema.possible_equivalence_class(second_attribute) or first_attribute == second_attribute:
            # useful for finding potential missing foreign keys
            # print("WARNING: Join condition %s is not based on foreign keys" % join_condition)
            return None, [], cardinality, None
        connection = schema.equivalence_connection(first_attribute, second_attribute)
        equivalence_connection = (first_table_instance, tuple(connection), second_table_instance)
        equivalence_connections.add(equivalence_connection)
    return PgExplain.merge_queries(child_queries[0], child_queries[1], equivalence_connections), pushed_down_predicates, cardinality, (join_type, child_ids[0], child_ids[1])

