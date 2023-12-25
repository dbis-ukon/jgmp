
from __future__ import annotations

import datetime

import sqlparse
from sqlparse import tokens
from sqlparse.sql import Comparison, Identifier, IdentifierList, Parenthesis, Where, Token
from sqlparse.tokens import Name
from query.predicatable import Predicatable
from query.predicate import Predicate
from schema.attribute import Attribute
from schema.comparison_operator import OPERATORS
from schema.sql.sql_foreign_key import SQLForeignKey
from schema.sql.sql_schema import SQLSchema
from query.sql.sql_join import SQLJoin
from query.sql.sql_table_instance import SQLTableInstance
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple, TypeVar
from query.graphlike_query import GraphlikeQuery
from schema.sql.sql_table import SQLTable


T = TypeVar('T')


def split_list(list: List[T], separator: Callable[[T], bool]) -> List[List[T]]:
    chunks = []
    chunk = []
    for element in list:
        if separator(element) and len(chunk) > 0:
            chunks.append(chunk)
            chunk = []
        else:
            chunk.append(element)
    if len(chunk) > 0:
        chunks.append(chunk)
    return chunks


def contains(list: List[T], condition: Callable[[T], bool]) -> bool:
    for element in list:
        if condition(element):
            return True
    return False


class SQLQuery(GraphlikeQuery):
    def __init__(self,
                 nodes: List[SQLTableInstance],
                 edges: List[Tuple[SQLTableInstance, SQLJoin, SQLTableInstance]],
                 cardinality_estimates: Optional[Dict[str, float]] = None,
                 name: Optional[str] = None,
                 id: Optional[int] = None) -> None:
        GraphlikeQuery.__init__(self,
                                "sql",
                                [node for node in nodes],
                                [edge for edge in edges],
                                cardinality_estimates=cardinality_estimates,
                                name=name,
                                query_id=id)

    def text(self) -> str:
        return self.sql()

    def sql(self, count: bool = False):
        if count:
            query = "SELECT COUNT(*)\nFROM "
        else:
            query = "SELECT *\nFROM "
        node_join_conditions = {}
        node_order = {}
        are_equivalent = {}
        should_equivalent = {}
        source_equivalent = {}

        for i, (node, edges) in enumerate(self.traverse_query()):
            node_order[node] = i
            if not node.virtual():
                node_join_conditions[node] = []
            updated = set()
            for from_node, edge, to_node in edges:
                foreign_key: SQLForeignKey = edge.labels()[0]
                for fk_attribute, pk_attribute in zip(foreign_key.foreign_key_attributes(), foreign_key.primary_key_attributes()):
                    if (from_node, fk_attribute) not in should_equivalent:
                        should_equivalent[(from_node, fk_attribute)] = {(from_node, fk_attribute)}
                        are_equivalent[(from_node, fk_attribute)] = {(from_node, fk_attribute)}
                    if (to_node, pk_attribute) not in should_equivalent:
                        should_equivalent[(to_node, pk_attribute)] = {(to_node, pk_attribute)}
                        are_equivalent[(to_node, pk_attribute)] = {(to_node, pk_attribute)}
                    if should_equivalent[(from_node, fk_attribute)] != should_equivalent[(to_node, pk_attribute)]:
                        union = should_equivalent[(from_node, fk_attribute)].union(should_equivalent[(to_node, pk_attribute)])
                        for eq_node, eq_attribute in union:
                            should_equivalent[(eq_node, eq_attribute)] = union
                        if not from_node.virtual() and not to_node.virtual():
                            are_union = are_equivalent[(from_node, fk_attribute)].union(are_equivalent[(to_node, pk_attribute)])
                            for eq_node, eq_attribute in are_union:
                                are_equivalent[(eq_node, eq_attribute)] = are_union
                            node_join_conditions[node].append("%s.%s = %s.%s" % (self._alias[from_node], fk_attribute.name(), self._alias[to_node], pk_attribute.name()))
                        else:
                            updated.add((from_node, fk_attribute))
                            updated.add((to_node, pk_attribute))
                        if not to_node.virtual():
                            source_equivalent[(from_node, fk_attribute)] = (to_node, pk_attribute)

            for up_node, up_attribute in updated:
                if not up_node.virtual():
                    for eq_node, eq_attribute in should_equivalent[(up_node, up_attribute)]:
                        if eq_node != up_node and not eq_node.virtual() and are_equivalent[(up_node, up_attribute)] != are_equivalent[(eq_node, eq_attribute)]:
                            while (eq_node, eq_attribute) in source_equivalent and are_equivalent[source_equivalent[(eq_node, eq_attribute)]] == are_equivalent[(eq_node, eq_attribute)]:
                                eq_node, eq_attribute = source_equivalent[(eq_node, eq_attribute)]
                            while (up_node, up_attribute) in source_equivalent and are_equivalent[source_equivalent[(up_node, up_attribute)]] == are_equivalent[(up_node, up_attribute)]:
                                up_node, up_attribute = source_equivalent[(up_node, up_attribute)]
                            union = are_equivalent[(up_node, up_attribute)].union(are_equivalent[(eq_node, eq_attribute)])
                            for union_node, union_attribute in union:
                                are_equivalent[(union_node, union_attribute)] = union
                            if node_order[up_node] < node_order[eq_node]:
                                cond_node = eq_node
                                cond_attribute = eq_attribute
                                other_node = up_node
                                other_attribute = up_attribute
                            else:
                                cond_node = up_node
                                cond_attribute = up_attribute
                                other_node = eq_node
                                other_attribute = eq_attribute
                            if (cond_node, cond_attribute) not in source_equivalent and (other_node, other_attribute) not in source_equivalent:
                                source_equivalent[(cond_node, cond_attribute)] = (other_node, other_attribute)
                            node_join_conditions[cond_node].append("%s.%s = %s.%s" % (self._alias[cond_node], cond_attribute.name(), self._alias[other_node], other_attribute.name()))

        from_strings = []
        for i, node in enumerate(node_join_conditions):
            alias_string = node.labels()[0].name() + " AS " + self._alias[node]
            if i == 0:
                from_strings.append(alias_string)
            elif len(node_join_conditions[node]) == 0:
                from_strings.append(alias_string + " ON TRUE")
            else:
                from_strings.append(alias_string + " ON " + " AND ".join(node_join_conditions[node]))

        query += "\n\tJOIN ".join(from_strings)
        query += self.predicate_string() + ";"
        return query

    @staticmethod
    def parse_sql(schema: SQLSchema, sql: str) -> SQLQuery:
        tables = {}
        predicates = {}

        parsed = sqlparse.parse(sql)[0]
        next_from = False
        for token in parsed.tokens:
            if token.is_keyword and token.normalized == "FROM":
                next_from = True
            elif isinstance(token, IdentifierList) and next_from:
                next_from = False
                for from_token in token.tokens:
                    if isinstance(from_token, Identifier):
                        table = next(it for it in from_token.tokens if it.ttype == Name).normalized.lower()
                        short = from_token.tokens[-1].normalized.lower()
                        tables[short] = schema.node(table)
                        predicates[short] = []
            elif isinstance(token, Where):
                raw_disjunctions = split_list(token.tokens[1:], (lambda t: t.is_keyword and t.normalized == "AND"))
                between = False
                disjunctions = []
                for disjunction in raw_disjunctions:
                    if between:
                        disjunctions[-1] += disjunction
                        between = False
                    else:
                        disjunctions.append(disjunction)
                        between = contains(disjunction, SQLQuery._between_condition)
                for disjunction in disjunctions:
                    parsed_disjunctions = SQLQuery._sql_disjunctions(tables, disjunction)
                    if parsed_disjunctions is not None:
                        short, disjunction_predicates = parsed_disjunctions
                        predicates[short] += disjunction_predicates

                table_instances = {}
                for short in tables:
                    table_instance = SQLTableInstance.build(schema, tables[short], predicates[short])
                    table_instances[short] = table_instance

                equalities = {}
                for disjunction in disjunctions:
                    if contains(disjunction, SQLQuery._join_condition):
                        comparison = next(token for token in disjunction if isinstance(token, Comparison))
                        left_short, left_column = SQLQuery._column_identifier(tables, comparison.left)
                        right_short, right_column = SQLQuery._column_identifier(tables, comparison.right)
                        left_table_instance = table_instances[left_short]
                        right_table_instance = table_instances[right_short]
                        _, left_column = SQLQuery._column_identifier(tables, comparison.left)
                        _, right_column = SQLQuery._column_identifier(tables, comparison.right)
                        equality = {(left_table_instance, left_column), (right_table_instance, right_column)}
                        if (left_table_instance, left_column) in equalities:
                            equality = equality | equalities[(left_table_instance, left_column)]
                        if (right_table_instance, right_column) in equalities:
                            equality = equality | equalities[(right_table_instance, right_column)]
                        for component in equality:
                            equalities[component] = equality

                joins = []
                components = list(equalities.keys())
                for component in components:
                    if component not in equalities:
                        continue
                    left_table_instance, left_column = component
                    for right_table_instance, right_column in equalities[component]:
                        foreign_key = schema.foreign_key(left_table_instance.table(), left_column, right_table_instance.table(), right_column)
                        if foreign_key is not None:
                            equalities[component].discard(component)
                            del equalities[component]
                            joins.append((left_table_instance, SQLJoin(foreign_key), right_table_instance))
                            break

                table_instances = [table_instances[short] for short in table_instances]
                components = list(equalities.keys())
                for component in components:
                    if component not in equalities:
                        continue
                    if len(equalities[component]) == 1:
                        del equalities[component]
                        continue
                    assert(len(equalities[component]) >= 2)
                    pk_tables = []
                    fks = {}
                    for left_table_instance, left_column in equalities[component]:
                        foreign_keys = schema.foreign_keys_from_column(left_table_instance.table(), left_column)
                        assert(len(foreign_keys) == 1)
                        pk_tables.append((foreign_keys[0][1]))
                        fks[(left_table_instance, left_column)] = foreign_keys[0][0]
                    assert(pk_tables.count(pk_tables[0]) == len(pk_tables))
                    virtual_table = pk_tables[0]
                    virtual_table_instance = SQLTableInstance(virtual_table, virtual_table.cardinality(), [], virtual=True)
                    table_instances.append(virtual_table_instance)
                    for left_table_instance, left_column in equalities[component]:
                        joins.append((left_table_instance, SQLJoin(fks[(left_table_instance, left_column)]), virtual_table_instance))
                        del equalities[(left_table_instance, left_column)]

                assert(len(list(equalities.keys())) == 0)

        return SQLQuery(table_instances, joins)

    @staticmethod
    def _comparison_condition(token: Token) -> bool:
        return isinstance(token, Comparison) and (not isinstance(token.right, Identifier) or token.right.normalized.endswith("'::timestamp"))

    @staticmethod
    def _like_condition(token: Token) -> bool:
        return token.normalized == "LIKE"

    @staticmethod
    def _in_condition(token: Token) -> bool:
        return isinstance(token, Comparison) and any([t.normalized == "IN" for t in token.tokens])

    @staticmethod
    def _is_condition(token: Token) -> bool:
        return token.normalized == "IS"

    @staticmethod
    def _between_condition(token: Token) -> bool:
        return token.normalized == "BETWEEN"

    @staticmethod
    def _join_condition(token: Token) -> bool:
        return isinstance(token, Comparison) and isinstance(token.left, Identifier) and isinstance(token.right, Identifier) and "." in token.right.normalized

    @staticmethod
    def _parenthesis_condition(token: Token) -> bool:
        return isinstance(token, Parenthesis)

    @staticmethod
    def _not_condition(token: Token) -> bool:
        return token.is_keyword and token.normalized == "NOT"

    @staticmethod
    def _sql_disjunctions(tables: Dict[str, SQLTable], sql_tokens: List[Token]) -> Optional[Tuple[str, List[List[Predicate]]]]:
        predicates = split_list(sql_tokens, (lambda t: t.is_keyword and t.normalized == "OR"))

        disjunction = []
        shorts = []
        multipliers = []
        for predicate in predicates:
            if contains(predicate, SQLQuery._in_condition):
                short, pred = SQLQuery._sql_in(tables, predicate)
                shorts.append(short)
                disjunction += pred
            elif contains(predicate, SQLQuery._like_condition):
                short, pred = SQLQuery._sql_like(tables, predicate)
                shorts.append(short)
                disjunction.append(pred)
            elif contains(predicate, SQLQuery._is_condition):
                short, pred = SQLQuery._sql_is(tables, predicate)
                shorts.append(short)
                disjunction.append(pred)
            elif contains(predicate, SQLQuery._parenthesis_condition):
                short, multiplier = SQLQuery._sql_nested_and(tables, next(token for token in predicate if SQLQuery._parenthesis_condition(token)).tokens)
                shorts.append(short)
                multipliers.append(multiplier)
            elif contains(predicate, SQLQuery._join_condition):
                return None
            else:
                short, pred = SQLQuery._sql_comparison(tables, next(token for token in predicate if SQLQuery._comparison_condition(token)))
                shorts.append(short)
                disjunction.append(pred)
        assert(all([short == shorts[0] for short in shorts]))

        disjunctions = [disjunction]
        for multiplier in multipliers:
            new_disjunctions = []
            for disjunction in disjunctions:
                for predicate in multiplier:
                    new_disjunctions.append(disjunction + [predicate])
            disjunctions = new_disjunctions

        return shorts[0], disjunctions

    @staticmethod
    def _sql_like(tables: Dict[str, SQLTable], sql_tokens: List[Token]) -> Tuple[str, Predicate]:
        identifier = next(token for token in sql_tokens if isinstance(token, Identifier))
        pattern = next(token for token in sql_tokens if token.ttype in tokens.String)

        positive = not contains(sql_tokens, SQLQuery._not_condition)

        short, attribute = SQLQuery._column_identifier(tables, identifier)

        value = pattern.normalized[1:-1]

        return short, Predicate(attribute, OPERATORS["LIKE"], value, positive=positive)

    @staticmethod
    def _sql_in(tables: Dict[str, SQLTable], sql_tokens: List[Token]) -> Tuple[str, List[Predicate]]:
        comparison = next(token for token in sql_tokens if isinstance(token, Comparison))
        identifier = next(token for token in comparison.tokens if isinstance(token, Identifier))
        parenthesis = next(token for token in comparison.tokens if isinstance(token, Parenthesis))

        positive = not contains(comparison.tokens, SQLQuery._not_condition)

        short, attribute = SQLQuery._column_identifier(tables, identifier)

        values = []
        if isinstance(parenthesis.tokens[1], IdentifierList):
            parenthesis_tokens = parenthesis.tokens[1].tokens
        else:
            parenthesis_tokens = parenthesis.tokens
        for token in parenthesis_tokens:
            if token.ttype in tokens.String:
                values.append(token.normalized[1:-1])

        predicates = [Predicate(attribute, OPERATORS["="], value, positive=positive) for value in values]
        return short, predicates

    @staticmethod
    def _sql_is(tables: Dict[str, SQLTable], sql_tokens: List[Token]) -> Tuple[str, Predicate]:
        identifier = next(token for token in sql_tokens if isinstance(token, Identifier))
        keyword = next(token for token in sql_tokens if token.is_keyword and "NULL" in token.normalized)

        short, attribute = SQLQuery._column_identifier(tables, identifier)

        positive = keyword.normalized == "NULL"

        return short, Predicate(attribute, OPERATORS["IS"], None, positive=positive)

    @staticmethod
    def _sql_between(tables: Dict[str, SQLTable], sql_tokens: List[Token]) -> Tuple[str, List[List[Predicate]]]:
        identifier = next(token for token in sql_tokens if isinstance(token, Identifier))
        short, attribute = SQLQuery._column_identifier(tables, identifier)

        positive = not contains(sql_tokens, SQLQuery._not_condition)

        values = []
        for token in sql_tokens:
            if token.ttype in tokens.Number:
                values.append(int(token.normalized))
            elif token.ttype in tokens.String:
                values.append(token.normalized[1:-1])

        assert(len(values) == 2)

        return short, [[Predicate(attribute, OPERATORS["<"], values[0], positive=not positive)],
                       [Predicate(attribute, OPERATORS[">"], values[1], positive=not positive)]]

    @staticmethod
    def _sql_comparison(tables: Dict[str, SQLTable], comparison: Comparison) -> Tuple[str, Predicate]:
        table_short, attribute = SQLQuery._column_identifier(tables, comparison.left)

        operator_symbol = next(token for token in comparison.tokens if token.ttype in tokens.Comparison).normalized

        right = comparison.right
        if right.ttype in tokens.String:
            value = right.normalized[1:-1]
        elif right.ttype in tokens.Number:
            value = int(right.normalized)
        elif right.normalized.endswith("'::timestamp"):
            value = datetime.datetime.strptime(right.normalized[1:-12], "%Y-%m-%d %H:%M:%S")
        else:
            raise NotImplementedError()

        return table_short, SQLQuery._sql_predicate(attribute, operator_symbol, value, True)

    @staticmethod
    def _sql_predicate(attribute: Attribute, operator_symbol: str, value: Any, positive: bool) -> Predicate:
        if operator_symbol in OPERATORS:
            operator = OPERATORS[operator_symbol]
        elif operator_symbol == "!=":
            operator = OPERATORS["="]
            positive = not positive
        elif operator_symbol == ">=":
            operator = OPERATORS["<"]
            positive = not positive
        elif operator_symbol == "<=":
            operator = OPERATORS[">"]
            positive = not positive
        elif operator_symbol == "like":
            operator = OPERATORS["LIKE"]
        elif operator_symbol.lower() == "not like":
            operator = OPERATORS["LIKE"]
            positive = not positive
        else:
            raise NotImplementedError()
        return Predicate(attribute, operator, value, positive=positive)

    @staticmethod
    def _sql_nested_and(tables: Dict[str, SQLTable], sql_tokens: List[Token]) -> Tuple[str, List[Predicate]]:
        predicates = split_list(sql_tokens, (lambda t: t.is_keyword and t.normalized == "AND"))

        conjunction = []
        shorts = []
        for predicate in predicates:
            if contains(predicate, SQLQuery._comparison_condition):
                short, pred = SQLQuery._sql_comparison(tables, next(token for token in predicate if SQLQuery._comparison_condition(token)))
                shorts.append(short)
                conjunction.append(pred)
            elif contains(predicate, SQLQuery._like_condition):
                short, pred = SQLQuery._sql_like(tables, predicate)
                shorts.append(short)
                conjunction.append(pred)
            elif contains(predicate, SQLQuery._is_condition):
                short, pred = SQLQuery._sql_is(tables, predicate)
                shorts.append(short)
                conjunction.append(pred)
            else:
                raise NotImplementedError()
        assert(all([short == shorts[0] for short in shorts]))
        return shorts[0], conjunction

    @staticmethod
    def _column_identifier(tables: Dict[str, SQLTable], token: Token) -> Tuple[str, Attribute]:
        table_short = token.tokens[0].normalized
        column_name = token.tokens[2].normalized.lower()
        table = tables[table_short]
        attribute = table.attribute(column_name)
        return table_short, attribute

    def build_subquery_nodes_edges(self,
                                   nodes: FrozenSet[SQLTableInstance]
                                   ) -> Tuple[List[SQLTableInstance], List[Tuple[SQLTableInstance, SQLJoin, SQLTableInstance]]]:
        edges = []
        own_nodes = set(self._nodes)
        neighboring = {}
        for node in nodes:
            assert(node in own_nodes)
            for edge, to_node in self._edges_from[node]:
                if to_node in nodes:
                    edges.append((node, edge, to_node))
                else:
                    assert(isinstance(edge, SQLJoin))
                    neighboring_key = (to_node, frozenset(edge.foreign_key().primary_key_attributes()))
                    if neighboring_key not in neighboring:
                        neighboring[neighboring_key] = []
                    neighboring[neighboring_key].append((node, edge))
        virtual_nodes = []
        for neighboring_key in neighboring:
            if len(neighboring[neighboring_key]) > 1:
                to_node, _ = neighboring_key
                assert(isinstance(to_node, SQLTableInstance))
                virtual_table = to_node.table()
                virtual_node = SQLTableInstance(virtual_table, virtual_table.cardinality(), [], virtual=True)
                virtual_nodes.append(virtual_node)
                for from_node, edge in neighboring[neighboring_key]:
                    edges.append((from_node, edge, virtual_node))

        return list(nodes) + virtual_nodes, edges

    def build_subquery(self, nodes: FrozenSet[SQLTableInstance], cardinality: int, id: Optional[int] = None) -> SQLQuery:
        nodes, edges = self.build_subquery_nodes_edges(nodes)
        return SQLQuery(nodes, edges, {"true": cardinality}, id=id)

    def shallow_copy(self) -> SQLQuery:
        query = SQLQuery(self._nodes, self.edges(), cardinality_estimates=self._cardinality_estimates)
        aliases = query.aliases()
        for node in self._alias:
            aliases[node] = self._alias[node]
        return query

    def deep_copy(self) -> Tuple[SQLQuery, Dict[Predicatable, Predicatable]]:
        copied_nodes = {node: node.copy() for node in self._nodes}
        copied_node_list = [copied_nodes[node] for node in self._nodes]
        copied_edge_list = []
        for from_node, edge, to_node in self.edges():
            copied_edge_list.append((copied_nodes[from_node], edge.copy(), copied_nodes[to_node]))
        return SQLQuery(copied_node_list, copied_edge_list, cardinality_estimates=self._cardinality_estimates.copy()), copied_nodes
