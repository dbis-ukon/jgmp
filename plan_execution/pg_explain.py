import datetime
import re
from typing import Dict, FrozenSet, Set, Tuple, List, Optional, Union
from plan.sql.join.join import Join
from plan.sql.scan.index_scan import IndexScan
from plan.sql.scan.scan import Scan
from plan.sql.sql_expression import SQLExpression
from query.predicate import ArbitraryPredicate, Predicate
from query.sql.sql_join import SQLJoin
from query.sql.sql_query import SQLQuery
from query.sql.sql_table_instance import SQLTableInstance
from schema.comparison_operator import DERIVED_OPERATORS, OPERATORS
from schema.data_type import DATATYPES
from schema.sql.sql_column import SQLColumn
from schema.sql.sql_schema import SQLSchema
from schema.sql.sql_table import SQLTable
from nltk import Tree


class PgExplain:
    def __init__(self, ignore_unary_operators: bool) -> None:
        self._ignore_unary_operators = ignore_unary_operators

    @staticmethod
    def build_subplan_queries(schema: SQLSchema, explain) -> List[SQLQuery]:
        subplan_queries, _, join_conditions, _ = PgExplain.build_subplan_queries_recursion(schema, explain[0]["Plan"])
        assert(len(join_conditions) == 0)
        return subplan_queries

    @staticmethod
    def build_subplan_queries_recursion(schema: SQLSchema, explain, parse: bool = True) -> Tuple[List[SQLQuery], Optional[SQLQuery], List[str], Dict[str, SQLTableInstance]]:
        all_subplan_queries = []
        child_queries = []
        orphaned_join_conditions = []
        child_table_instances = {}
        prefered_child = {}
        if "Plans" in explain:
            possible = True
            for i, explain_child in enumerate(explain["Plans"]):
                child_subplans, child_query, join_condition, child_table_instance = PgExplain.build_subplan_queries_recursion(schema, explain_child)
                all_subplan_queries += child_subplans
                orphaned_join_conditions += join_condition
                child_table_instances = child_table_instances | child_table_instance
                for jc in join_condition:
                    prefered_child[jc] = i
                if child_query is not None:
                    child_queries.append(child_query)
                else:
                    possible = False
            if not possible:
                return all_subplan_queries, None, [], child_table_instances

        # unsupported operators
        if explain["Node Type"] in ["Aggregate", "Append", "SetOp", "Unique", "WindowAgg", "Limit", "Subquery Scan"]:
            return all_subplan_queries, None, [], child_table_instances

        # purely physical operators
        if explain["Node Type"] in ["Sort", "Materialize", "Hash", "Gather", "Gather Merge", "Memoize", "Incremental Sort"]:
            return all_subplan_queries, child_queries[0], orphaned_join_conditions, child_table_instances

        if "Join Type" in explain:
            if not explain["Join Type"] == "Inner":
                return all_subplan_queries, None, [], child_table_instances
            join_conditions = orphaned_join_conditions
            if "Join Filter" in explain:
                join_condition = explain["Join Filter"]
                join_conditions += PgExplain._split_condition(join_condition)
            if "Hash Cond" in explain:
                join_condition = explain["Hash Cond"]
                join_conditions += PgExplain._split_condition(join_condition)
            if "Merge Cond" in explain:
                join_condition = explain["Merge Cond"]
                join_conditions += PgExplain._split_condition(join_condition)
            table_instances = child_queries[0].nodes() + child_queries[1].nodes()
            first_aliases = {v: k for k, v in child_queries[0].aliases().items() if not k.virtual()}
            second_aliases = {v: k for k, v in child_queries[1].aliases().items() if not k.virtual()}
            # attempt to get tables and columns of join conditions
            equivalence_connections = set()
            orphaned_join_conditions = []
            for join_condition in join_conditions:
                if join_condition in prefered_child:
                    child_index = prefered_child[join_condition]
                else:
                    child_index = None
                try:
                    left, right = join_condition[1:-1].split(" = ")
                    if "." in left:
                        left_alias, left_column = left.split(".")
                    else:
                        left_alias = PgExplain._find_alias(left, first_aliases, second_aliases, child_index)
                        left_column = left
                    if "." in right:
                        right_alias, right_column = right.split(".")
                    else:
                        right_alias = PgExplain._find_alias(right, first_aliases, second_aliases, child_index)
                        right_column = right
                except:
                    return all_subplan_queries, None, [], child_table_instances
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
                    orphaned_join_conditions.append(join_condition)
                    continue
                first_table_instance = first_aliases[first_alias]
                first_table = first_table_instance.table()
                first_attribute = first_table.attribute(first_column)
                second_table_instance = second_aliases[second_alias]
                second_table = second_table_instance.table()
                second_attribute = second_table.attribute(second_column)
                if not first_attribute in schema.possible_equivalence_class(second_attribute) or first_attribute == second_attribute:
                    return all_subplan_queries, None, [], child_table_instances
                connection = schema.equivalence_connection(first_attribute, second_attribute)
                equivalence_connection = (first_table_instance, tuple(connection), second_table_instance)
                equivalence_connections.add(equivalence_connection)
            query = PgExplain.merge_queries(child_queries[0], child_queries[1], equivalence_connections)
            if len(orphaned_join_conditions) == 0:
                return all_subplan_queries + [query], query, [], child_table_instances
            else:
                return all_subplan_queries, query, orphaned_join_conditions, child_table_instances

        if explain["Node Type"] == "CTE Scan":
            return all_subplan_queries, None, [], child_table_instances

        if "Scan" in explain["Node Type"]:
            predicates = []
            if explain["Node Type"] == "Bitmap Index Scan":
                table = schema.index_to_table(explain["Index Name"])
                table_alias = table.name()
            elif explain["Node Type"] == "Bitmap Heap Scan":
                assert(len(child_queries) == 1)
                child_query_nodes = child_queries[0].nodes()
                assert(len(child_query_nodes) == 1)
                child_query_node = child_query_nodes[0]
                table = child_query_node.table()
                predicates += child_query_node.predicates()
                table_alias = explain["Alias"]
            else:
                table_name = explain["Relation Name"]
                table = schema.node(table_name)
                table_alias = explain["Alias"]

            attributes = [att.name() for att in table.attributes()]
            if "Filter" in explain:
                predicates += PgExplain._split_filter(explain["Filter"], table_alias, attributes)
            if "Index Cond" in explain:
                predicates += PgExplain._split_filter(explain["Index Cond"], table_alias, attributes)

            if len(predicates) == 0:
                cardinality = table.cardinality()
                happy_predicates = []
                orphaned_predicates = []
            else:
                connection = schema.connection()
                cursor = connection.cursor()

                happy_predicates = []
                orphaned_predicates = []
                for predicate in predicates:
                    test_query = "EXPLAIN (FORMAT JSON) " + SQLTableInstance.sql(table, [predicate], alias=table_alias)
                    try:
                        cursor.execute(test_query)
                        happy_predicates.append(predicate)
                    except:
                        cursor.close()
                        connection.rollback()
                        cursor = connection.cursor()
                        orphaned_predicates.append(predicate)

                cardinality_query = "EXPLAIN (FORMAT JSON) " + SQLTableInstance.sql(table, happy_predicates, alias=table_alias)
                cursor.execute(cardinality_query)
                cardinality = cursor.fetchone()[0][0]["Plan"]["Plan Rows"]
                cursor.close()

            if parse:
                happy_predicates = PgExplain.parse_arbitrary_predicates(table, table_alias, happy_predicates)
            table_instance = SQLTableInstance(table, cardinality, happy_predicates)
            query = SQLQuery([table_instance], [])
            query.aliases()[table_instance] = table_alias
            if len(orphaned_predicates) == 0 and len(orphaned_join_conditions) == 0:
                return [query], query, [], {table_alias: table_instance}
            else:
                return [], query, [op[0].query_string("") for op in orphaned_predicates] + orphaned_join_conditions, {table_alias: table_instance}

        raise NotImplementedError("Unknown node type: " + explain["Node Type"])

    @staticmethod
    def merge_queries(query_a: SQLQuery, query_b: SQLQuery, connections: Set[Tuple[SQLTableInstance, tuple, SQLTableInstance]]) -> SQLQuery:
        table_instances = query_a.nodes() + query_b.nodes()
        joins = query_a.edges() + query_b.edges()
        old_aliases = query_a.aliases() | query_b.aliases()
        for first_table_instance, connection, second_table_instance in connections:
            foreign_key = None
            direction = None
            last_table_instance = first_table_instance
            for elem in connection:
                if isinstance(elem, SQLTable):
                    assert (foreign_key is not None and direction is not None)
                    next_table_instance = SQLTableInstance(elem, elem.cardinality(), [], virtual=True)
                    table_instances.append(next_table_instance)
                    join = SQLJoin(foreign_key)
                    if direction:
                        uno_table_instance = last_table_instance
                        dos_table_instance = next_table_instance
                    else:
                        uno_table_instance = next_table_instance
                        dos_table_instance = last_table_instance
                    joins.append((uno_table_instance, join, dos_table_instance))
                    table_instances, joins, uno_table_instance, dos_table_instance = PgExplain._merge_virtual_nodes(table_instances, joins, uno_table_instance,
                                                                                                                    join, dos_table_instance)
                    if direction:
                        next_table_instance = dos_table_instance
                    else:
                        next_table_instance = uno_table_instance
                    last_table_instance = next_table_instance
                else:
                    foreign_key, direction = elem

            join = SQLJoin(foreign_key)
            if direction:
                uno_table_instance = last_table_instance
                dos_table_instance = second_table_instance
            else:
                uno_table_instance = second_table_instance
                dos_table_instance = last_table_instance
            joins.append((uno_table_instance, join, dos_table_instance))
            table_instances, joins, _, _ = PgExplain._merge_virtual_nodes(table_instances, joins, uno_table_instance, join, dos_table_instance)

        query = SQLQuery(table_instances, joins)
        new_aliases = query.aliases()
        for node in new_aliases:
            if node in old_aliases:
                new_aliases[node] = old_aliases[node]
        return query

    @staticmethod
    def parse_arbitrary_predicates(table: SQLTable, alias: str, predicates: List[List[ArbitraryPredicate]], injection_mode: bool = False) -> List[List[Union[Predicate, ArbitraryPredicate]]]:
        conjunction = {"operator": "AND", "predicates": []}
        for disjunction in predicates:
            assert(len(disjunction) == 1)
            parsed = PgExplain.parse_arbitrary_predicate(table, alias, disjunction[0].query_string(""), injection_mode)
            if parsed is None:
                conjunction["predicates"].append(disjunction[0])
            else:
                conjunction["predicates"].append(parsed)
        return PgExplain.normal_form_predicates(conjunction)

    @staticmethod
    def parse_arbitrary_predicate(table: SQLTable, alias: str, predicate: str, injection_mode: bool) -> Optional[Union[dict, Predicate]]:
        parse_tree = Tree.fromstring(predicate)
        if PgExplain._is_conjunction(parse_tree):
            predicates = [PgExplain.parse_arbitrary_predicate(table, alias, str(pt), injection_mode) for i, pt in enumerate(parse_tree) if i % 2 == 0]
            if None in predicates:
                return None
            return {"operator": "AND", "predicates": predicates}
        elif PgExplain._is_disjunction(parse_tree):
            predicates = [PgExplain.parse_arbitrary_predicate(table, alias, str(pt), injection_mode) for i, pt in enumerate(parse_tree) if i % 2 == 0]
            if None in predicates:
                return None
            return {"operator": "OR", "predicates": predicates}
        if isinstance(parse_tree[1], str) and parse_tree[1][0:2] == "::":
            predicate_string = "(" + parse_tree[0].label() + " " + " ".join([str(pt) for pt in parse_tree[2:]]) + ")"
            parse_tree = Tree.fromstring(predicate_string)
        if len(parse_tree) == 3 and parse_tree[0] == "=" and parse_tree[1] == "ANY":
            column = PgExplain._get_column(table, alias, parse_tree.label())
            option_string = str(parse_tree[2])[1:-1]
            if "::" in option_string:
                options, cast = option_string.split("::")
                cast = cast.strip()
                options = options[2:-2].split(",")
                if cast in ["bpchar[]", "text[]", "integer[]"]:
                    values = [column.data_type().python_type()(option) for option in options]
                elif cast == "date[]":
                    values = [datetime.datetime.strptime(option, "%Y-%m-%d") for option in options]
                else:
                    raise NotImplementedError("Unknown cast: " + cast)
            elif injection_mode:
                options = option_string.split(",")
                values = []
                for option in options:
                    option = option.strip()
                    # test if option begins with ' and ends with '
                    if option[0] == "'" and option[-1] == "'":
                        option = option[1:-1]
                    data_type = column.data_type()
                    if data_type == DATATYPES["date"]:
                        value = datetime.datetime.strptime(option, "%Y-%m-%d")
                    else:
                        value = column.data_type().python_type()(option)
                    values.append(value)
            else:
                raise NotImplementedError
            predicates = []
            for value in values:
                predicates.append(Predicate(column, OPERATORS["="], value))
            return {"operator": "OR", "predicates": predicates}
        elif len(parse_tree) > 1 and isinstance(parse_tree[0], str) and parse_tree[0] in DERIVED_OPERATORS:
            column = PgExplain._get_column(table, alias, parse_tree.label())
            operator, positive = DERIVED_OPERATORS[parse_tree[0]]
            column_type = column.data_type()
            value_string = " ".join(parse_tree[1:])
            if "::" in value_string:
                value, cast = value_string.split("::")
                cast = cast.strip()
                if cast in ["numeric", "bpchar", "text"]:
                    value = column_type.python_type()(value[1:-1])
                elif cast == "timestamp without time zone":
                    value = datetime.datetime.strptime(value[1:-1], "%Y-%m-%d %H:%M:%S")
                    if column_type == DATATYPES["date"]:
                        value = value.date()
                elif cast == "date":
                    value = datetime.datetime.strptime(value[1:-1], "%Y-%m-%d").date()
                else:
                    raise NotImplementedError("Unknown cast: " + cast)
            else:
                value_string = value_string.strip()
                if value_string[0] == "'" and value_string[-1] == "'":
                    value_string = value_string[1:-1]
                data_type = column.data_type()
                if data_type == DATATYPES["date"]:
                    value_string = value_string[0:10]
                    value = datetime.datetime.strptime(value_string, "%Y-%m-%d").date()
                else:
                    value = column.data_type().python_type()(value_string)
            return Predicate(column, operator, value, positive=positive)
        return None

    @staticmethod
    def _get_column(table: SQLTable, alias: str, string: str) -> SQLColumn:
        id_alias, id_column = string.split(".")
        assert(id_alias == alias)
        return table.attribute(id_column)

    @staticmethod
    def normal_form_predicates(predicate_dict: dict) -> List[List[Union[Predicate, ArbitraryPredicate]]]:
        predicate_dict = PgExplain._flatten(predicate_dict)
        predicate_dict = PgExplain._apply_distributive_law(predicate_dict)
        if isinstance(predicate_dict, Predicate) or isinstance(predicate_dict, ArbitraryPredicate):
            return [[predicate_dict]]
        elif predicate_dict["operator"] == "AND":
            return [PgExplain._disjunction_to_list(disjunction) for disjunction in predicate_dict["predicates"]]
        else:
            return [PgExplain._disjunction_to_list(predicate_dict)]

    @staticmethod
    def _disjunction_to_list(predicate_dict: Union[dict, Predicate, ArbitraryPredicate]) -> List[Union[Predicate, ArbitraryPredicate]]:
        if isinstance(predicate_dict, Predicate) or isinstance(predicate_dict, ArbitraryPredicate):
            return [predicate_dict]
        else:
            assert(predicate_dict["operator"] == "OR")
            for predicate in predicate_dict["predicates"]:
                assert(isinstance(predicate, Predicate) or isinstance(predicate, ArbitraryPredicate))
            return predicate_dict["predicates"]

    @staticmethod
    def _apply_distributive_law(predicate_dict: Union[dict, Predicate, ArbitraryPredicate]) -> Union[dict, Predicate, ArbitraryPredicate]:
        if isinstance(predicate_dict, Predicate) or isinstance(predicate_dict, ArbitraryPredicate):
            return predicate_dict
        elif predicate_dict["operator"] == "AND":
            return PgExplain._flatten({"operator": "AND", "predicates": [PgExplain._apply_distributive_law(subpredicate) for subpredicate in predicate_dict["predicates"]]})
        assert(predicate_dict["operator"] == "OR")
        disjunctions = [[]]
        for predicate in predicate_dict["predicates"]:
            if isinstance(predicate, dict) and predicate["operator"] == "AND":
                subpredicates = predicate["predicates"]
            else:
                subpredicates = [predicate]
            new_disjunctions = []
            for disjunction in disjunctions:
                for subpredicate in subpredicates:
                    new_disjunctions.append(disjunction + [subpredicate])
            disjunctions = new_disjunctions
        if len(disjunctions) == 1:
            return PgExplain._flatten({"operator": "AND", "predicates": [{"operator": "OR", "predicates": [PgExplain._apply_distributive_law(predicate) for predicate in disjunction]} for disjunction in disjunctions]})
        else:
            return PgExplain._flatten({"operator": "AND", "predicates": [PgExplain._apply_distributive_law({"operator": "OR", "predicates": disjunction}) for disjunction in disjunctions]})

    @staticmethod
    def _flatten(predicate_dict: Union[dict, Predicate, ArbitraryPredicate]) -> Union[dict, Predicate, ArbitraryPredicate]:
        if isinstance(predicate_dict, Predicate) or isinstance(predicate_dict, ArbitraryPredicate):
            return predicate_dict
        elif len(predicate_dict["predicates"]) == 1:
            return PgExplain._flatten(predicate_dict["predicates"][0])
        else:
            dict_type = predicate_dict["operator"]
            predicates = []
            changed = False
            for predicate in predicate_dict["predicates"]:
                if isinstance(predicate, dict) and predicate["operator"] == dict_type:
                    predicates += predicate["predicates"]
                    changed = True
                else:
                    predicates.append(predicate)
            if changed:
                return PgExplain._flatten({"operator": dict_type, "predicates": predicates})
            else:
                return {"operator": dict_type, "predicates": [PgExplain._flatten(predicate) for predicate in predicates]}


    @staticmethod
    def _find_alias(column_name: str, first_aliases: Dict[str, SQLTableInstance], second_aliases: Dict[str, SQLTableInstance], child_index: Optional[int]) -> Optional[str]:
        found_alias_first = []
        for alias, table_instance in first_aliases.items():
            if table_instance.table().attribute_exists(column_name):
                found_alias_first.append(alias)
        found_alias_second = []
        for alias, table_instance in second_aliases.items():
            if table_instance.table().attribute_exists(column_name):
                found_alias_second.append(alias)
        found_alias = found_alias_first + found_alias_second
        if len(found_alias) == 0:
            return None
        elif len(found_alias) == 1:
            return found_alias[0]
        elif child_index is None:
            return None
        elif child_index == 0 and len(found_alias_first) == 1:
            return found_alias_first[0]
        elif child_index == 1 and len(found_alias_second) == 1:
            return found_alias_second[0]
        else:
            return None

    @staticmethod
    def _merge_virtual_nodes(table_instances: List[SQLTableInstance],
                             joins: List[Tuple[SQLTableInstance, SQLJoin, SQLTableInstance]],
                             first_table_instance: SQLTableInstance,
                             added_join: SQLJoin,
                             second_table_instance: SQLTableInstance
                             ) -> Tuple[List[SQLTableInstance], List[Tuple[SQLTableInstance, SQLJoin, SQLTableInstance]], SQLTableInstance, SQLTableInstance]:
        merge = None
        for from_node, join, to_node in joins:
            if from_node == first_table_instance and join != added_join and join.foreign_key() == added_join.foreign_key() and to_node.virtual():
                merge = (second_table_instance, to_node)
                break
            elif from_node == first_table_instance and join != added_join and join.foreign_key() == added_join.foreign_key() and second_table_instance.virtual():
                merge = (to_node, second_table_instance)
                second_table_instance = to_node
                break
            elif to_node == second_table_instance and join != added_join and join.foreign_key() == added_join.foreign_key() and from_node.virtual():
                merge = (first_table_instance, from_node)
                break
            elif to_node == second_table_instance and join != added_join and join.foreign_key() == added_join.foreign_key() and first_table_instance.virtual():
                merge = (from_node, first_table_instance)
                first_table_instance = from_node
                break
        if merge is None:
            return table_instances, joins, first_table_instance, second_table_instance
        new_table_instances = [ti for ti in table_instances if ti != merge[1]]
        new_joins = []
        new_foreign_keys = {}
        for from_node, join, to_node in joins:
            if from_node == merge[1]:
                from_node = merge[0]
            if to_node == merge[1]:
                to_node = merge[0]
            if (from_node, join.foreign_key(), to_node) not in new_foreign_keys:
                new_joins.append((from_node, join, to_node))
                new_foreign_keys[(from_node, join.foreign_key(), to_node)] = join
            elif join == added_join:
                added_join = new_foreign_keys[(from_node, join.foreign_key(), to_node)]
        return PgExplain._merge_virtual_nodes(new_table_instances, new_joins, first_table_instance, added_join, second_table_instance)

    @staticmethod
    def _split_filter(predicate_string: str, alias: str, attributes: List[str]) -> List[List[ArbitraryPredicate]]:
        predicates_strings = PgExplain._split_condition(predicate_string)
        predicates = []
        for predicate_string in predicates_strings:
            for attribute in attributes:
                # find all occurrences of attribute in predicate_string without leading dot
                occurrences = [m.start() for m in re.finditer("(?<!_a-zA-Z\.)" + attribute, predicate_string)]
                for occurence in reversed(occurrences):
                    # add alias. to beginning of attribute
                    predicate_string = predicate_string[:occurence] + alias + "." + predicate_string[occurence:]
            predicates.append([ArbitraryPredicate(predicate_string)])
        return predicates

    @staticmethod
    def _split_condition(condition_string: str) -> List[str]:
        parse_tree = Tree.fromstring(condition_string)
        elements = []
        if PgExplain._is_conjunction(parse_tree):
            for i, element in enumerate(parse_tree):
                if i % 2 == 0:
                    if isinstance(element, Tree):
                        elements.append(element._pformat_flat("", "()", False))
                    else:
                        elements.append(str(element))
        else:
            elements.append(parse_tree._pformat_flat("", "()", False))
        return elements
        assert (condition_string[0] == "(" and condition_string[-1] == ")" and condition_string.count("(") == condition_string.count(")"))
        predicate_string = condition_string[1:-1]
        open_parentheses = 0
        predicates_strings = []
        for i in range(len(predicate_string)):
            char = predicate_string[i]
            if char == "(":
                if open_parentheses == 0:
                    predicates_strings.append("")
                open_parentheses += 1
                continue
            elif char == ")":
                open_parentheses -= 1
            if open_parentheses > 0:
                predicates_strings[-1] += char
        assert(open_parentheses == 0)
        return predicates_strings

    @staticmethod
    def _is_conjunction(parse_tree: Tree):
        for i in range(len(parse_tree)):
            if i % 2 == 1 and parse_tree[i] != "AND":
                return False
        return True

    @staticmethod
    def _is_disjunction(parse_tree: Tree):
        for i in range(len(parse_tree)):
            if i % 2 == 1 and parse_tree[i] != "OR":
                return False
        return True

    def extract_table_sets(self, query: SQLQuery, explain, index_scans: bool = True) -> List[FrozenSet[SQLTableInstance]]:
        plan = self.extract(query, explain)
        table_sets, _ = PgExplain._extract_table_sets_recursion(plan, index_scans)
        return list(table_sets)

    @staticmethod
    def _extract_table_sets_recursion(plan: SQLExpression, index_scans: bool) -> Tuple[Set[FrozenSet[SQLTableInstance]], List[SQLTableInstance]]:
        operator = plan.operator()
        if isinstance(operator, Scan):
            table_instance = operator.table()
            if isinstance(operator, IndexScan) and not index_scans:
                return set(), [table_instance]
            return set([frozenset([table_instance])]), [table_instance]

        children_sets = set()
        children_table_list = []
        for child in plan.children():
            child_set, child_table_list = PgExplain._extract_table_sets_recursion(child, index_scans)
            children_sets = children_sets.union(child_set)
            children_table_list += child_table_list

        children_sets.add(frozenset(children_table_list))
        return children_sets, children_table_list

    def extract(self, query: SQLQuery, explain) -> SQLExpression:
        return self._extract_recursion(query, explain["Plan"])

    def _extract_recursion(self, query: SQLQuery, explain) -> SQLExpression:  # TODO: Physical Scan and joins and other operators
        children = []
        if "Plans" in explain:
            for explain_child in explain["Plans"]:
                children.append(self._extract_recursion(query, explain_child))

        node_type = explain["Node Type"]
        if node_type == "Seq Scan" or node_type == "Index Scan" or node_type == "Index Only Scan" or node_type == "Bitmap Heap Scan":
            table_instance = query.predicatable(explain["Alias"])
            assert(isinstance(table_instance, SQLTableInstance))
            if node_type == "Index Scan" or node_type == "Index Only Scan":
                table = table_instance.table()
                key_columns = table.key_columns()
                assert(len(key_columns) == 1)  # TODO: composite primary keys
                operator = IndexScan(table_instance, key_columns[0])
            else:
                operator = Scan(table_instance)
            if node_type == "Bitmap Heap Scan":
                children = []
        elif node_type == "Hash Join" or node_type == "Merge Join" or node_type == "Nested Loop":
            join_conditions = []  # TODO
            operator = Join(join_conditions)
        elif len(children) == 1:
            return children[0]
        elif node_type == "Bitmap Index Scan":
            return None
        else:
            raise NotImplementedError("Unknown node type %s" % node_type)
        return SQLExpression(operator, children)

    def execution_time(self, explain) -> float:
        return explain["Execution Time"]

    def cardinalities(self, query: SQLQuery, explain) -> Dict[FrozenSet[SQLTableInstance], int]:
        _, cardinalities = self._cardinality_recursion(query, explain["Plan"])

        return cardinalities

    def _cardinality_recursion(self, query: SQLQuery, explain) -> Tuple[Set[SQLTableInstance], Dict[FrozenSet[SQLTableInstance], int]]:
        tables = set()
        cardinalities = {}
        if "Alias" in explain:
            alias = explain["Alias"]
            tables.add(query.predicatable(alias))
        else:
            for plan in explain["Plans"]:
                child_tables, child_cardinalities = self._cardinality_recursion(query, plan)
                tables.update(child_tables)
                cardinalities.update(child_cardinalities)
        tables_frozen = frozenset(tables)
        if self._is_unfiltered_cardinality(query, tables_frozen, explain):
            cardinalities[tables_frozen] = explain["Actual Rows"] * explain["Actual Loops"]
        return tables, cardinalities

    def _is_unfiltered_cardinality(self, query: SQLQuery, tables_frozen: FrozenSet[SQLTableInstance], explain) -> bool:
        if len(tables_frozen) == len(query.nodes()):
            return True

        conditions = []
        if "Index Cond" in explain:
            conditions.append(explain["Index Cond"])
        if "Filter" in explain and (explain["Rows Removed by Filter"] > 0 or "Index Cond" in explain):
            conditions.append(explain["Filter"])

        if len(tables_frozen) > 1:
            return len(conditions) == 0

        conditions = " AND ".join(conditions)

        return len(next(iter(tables_frozen)).predicates()) == 1 + conditions.count(" AND ")

    def contains_gather(self, explain) -> bool:
        return self._gather_recursion(explain["Plan"])

    def _gather_recursion(self, explain) -> bool:
        if explain["Node Type"] == "Gather":
            return True

        if "Plans" in explain:
            for plan in explain["Plans"]:
                if self._gather_recursion(plan):
                    return True

        return False
