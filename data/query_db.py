
from typing import Any, Dict, FrozenSet, Iterator, List, Optional, Tuple, Union
import psycopg2
from data.wrap_value import wrap_value
from query.graphlike_query import GraphlikeQuery
from query.predicatable import Predicatable
from query.predicate import Predicate
from query.query_edge import EdgeDirection, QueryEdge
from query.query_node import QueryNode
from query.sql.sql_join import SQLJoin
from query.sql.sql_query import SQLQuery
from query.sql.sql_table_instance import SQLTableInstance
from schema.attributable import Attributable
from schema.comparison_operator import OPERATORS
from schema.graphlike_schema import GraphlikeSchema
from schema.schema_edge import SchemaEdge
from schema.schema_node import SchemaNode
import datetime


class QueryDB:
    def __init__(self, name: str, port: Optional[int] = None) -> None:
        if port is None:
            self._connection = psycopg2.connect(host="localhost", database=name, user="postgres", password="postgres")
        else:
            self._connection = psycopg2.connect(host="localhost", database=name, user="postgres", password="postgres", port=port)

        self._query_languages = self._get_name_dict("query_languages")
        self._directions = self._get_name_dict("directions")
        self._operators = self._get_name_dict("comparison_operators")
        self._data_roles = self._get_name_dict("data_roles")

    def _get_name_dict(self, table: str) -> Dict[str, id]:
        name_dict = {}

        cursor = self._connection.cursor()
        query = "SELECT id, name FROM %s;" % table
        cursor.execute(query)
        for id, name in cursor.fetchall():
            name_dict[name] = id
        cursor.close()
        return name_dict

    def commit(self):
        self._connection.commit()

    def get_query_set(self, query_id: int) -> Optional[str]:
        cursor = self._connection.cursor()
        query = "SELECT name FROM query_sets qs JOIN queries q ON q.set_id = qs.id WHERE q.id = %s;" % query_id
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result is not None else None

    def load_query_set(self,
                       schema: GraphlikeSchema,
                       name: str,
                       estimator_id: int,
                       cardinality: bool = False,
                       all_estimates: bool = False,
                       only_with_true_cardinality: bool = False
                       ) -> List[GraphlikeQuery]:
        print("Loading queries")
        queries, _, _ = self._load_query_set_dict(schema,
                                                  estimator_id,
                                                  name,
                                                  cardinality,
                                                  all_estimates=all_estimates,
                                                  only_with_true_cardinality=only_with_true_cardinality)
        return [queries[query_id] for query_id in queries]

    def _load_query_set_dict(self,
                             schema: GraphlikeSchema,
                             estimator_id: int,
                             name: str,
                             cardinality: bool,
                             all_estimates: bool = False,
                             only_with_true_cardinality: bool = False
                             ) -> Tuple[Dict[int, GraphlikeQuery], Dict[int, QueryNode], Dict[GraphlikeQuery, str]]:
        set_id = self._get("query_sets", {"name": name})
        cursor = self._connection.cursor()
        language_query = """SELECT ql.name
                            FROM query_sets AS qs
                            JOIN databases AS d ON d.id = qs.database_id
                            JOIN query_languages AS ql ON ql.id = d.language_id
                            WHERE qs.name = %s;""" % wrap_value(name)
        cursor.execute(language_query)
        language = cursor.fetchone()[0]
        name_query = """SELECT q.id, q.name
                        FROM query_sets AS qs
                        JOIN queries AS q ON q.set_id = qs.id
                        WHERE qs.name = %s;""" % wrap_value(name)
        cursor.execute(name_query)
        names = {}
        for id, name in cursor.fetchall():
            if name is None:
                names[id] = "unnamed query"
            else:
                names[id] = name
        cursor.close()
        node_labels = self._load_node_labels(schema, set_id)
        edge_labels = self._load_edge_labels(schema, set_id)
        node_predicates = self._load_node_predicates(node_labels, set_id)
        edge_predicates = self._load_edge_predicates(edge_labels, set_id)
        query_nodes, nodes = self._load_nodes(set_id, node_labels, node_predicates, language, estimator_id)
        edges = self._load_edges(set_id, nodes, edge_labels, edge_predicates, language)
        if cardinality:
            cardinalities = self._load_cardinalities(set_id, all_estimates)
        else:
            cardinalities = {}

        queries = {}
        query_names = {}
        for query_id in query_nodes:
            qn = query_nodes[query_id]
            if query_id in edges:
                qe = edges[query_id]
            else:
                qe = []
            if language == "sql":
                if query_id in names:
                    query = SQLQuery(qn, qe, name=names[query_id], id=query_id)
                else:
                    query = SQLQuery(qn, qe, id=query_id)
            else:
                raise NotImplementedError()
            queries[query_id] = query
            query_names[query] = names[query_id]
        for query_id, estimator in cardinalities:
            queries[query_id].cardinality_estimates()[estimator] = cardinalities[(query_id, estimator)]
        if only_with_true_cardinality:
            queries = {query_id: queries[query_id] for query_id in queries if "true" in queries[query_id].cardinality_estimates()}
        return queries, nodes, query_names

    def load_query(self,
                   schema: GraphlikeSchema,
                   query_id: int,
                   estimator_id: int,
                   cardinality: bool,
                   all_estimates: bool = False) -> GraphlikeQuery:
        # TODO: broken for fk-fk joins
        cursor = self._connection.cursor()
        language_query = """SELECT ql.name
                                    FROM queries AS q
                                    JOIN query_sets AS qs ON qs.id = q.set_id
                                    JOIN databases AS d ON d.id = qs.database_id
                                    JOIN query_languages AS ql ON ql.id = d.language_id
                                    WHERE q.id = %d;""" % query_id
        cursor.execute(language_query)
        language = cursor.fetchone()[0]
        name_query = """SELECT q.name
                                FROM queries AS q
                                WHERE q.id  = %d;""" % query_id
        cursor.execute(name_query)
        name = cursor.fetchone()[0]
        cursor.close()
        node_labels = self._load_node_labels_query(schema, query_id)
        edge_labels = self._load_edge_labels_query(schema, query_id)
        node_predicates = self._load_node_predicates_query(node_labels, query_id)
        edge_predicates = self._load_edge_predicates_query(edge_labels, query_id)
        nodes = self._load_nodes_query(query_id, node_labels, node_predicates, language, estimator_id)
        edges = self._load_edges_query(query_id, nodes, edge_labels, edge_predicates, language)
        if cardinality:
            cardinalities = self._load_cardinalities_query(query_id, all_estimates)
        else:
            cardinalities = {}

        queries = {}
        query_names = {}
        qn = [nodes[node_id] for node_id in nodes]
        if query_id in edges:
            qe = edges[query_id]
        else:
            qe = []
        if language == "sql":
            if name is not None:
                query = SQLQuery(qn, qe, name=name, id=query_id)
            else:
                query = SQLQuery(qn, qe, id=query_id)
        else:
            raise NotImplementedError()
        queries[query_id] = query
        query_names[query] = name
        for estimator in cardinalities:
            query.cardinality_estimates()[estimator] = cardinalities[estimator]
        return query

    def load_group_cardinalities(self,
                                 schema: GraphlikeSchema,
                                 name: str,
                                 estimator_id: int,
                                 only_annotated: bool,
                                 query_cardinality_estimator: str = "true",
                                 all_group_ids:bool = False
                                 ) -> Tuple[List[GraphlikeQuery],
                                            Dict[GraphlikeQuery, int],
                                            Dict[QueryNode, int],
                                            Dict[GraphlikeQuery, Dict[FrozenSet[QueryNode], int]],
                                            int,
                                            int,
                                            Dict[GraphlikeQuery, str],
                                            Dict[GraphlikeQuery, Dict[FrozenSet[QueryNode], int]]]:
        queries, nodes, query_names = self._load_query_set_dict(schema, estimator_id, name, True)
        set_query = """SELECT qs.id, qs.database_id
                       FROM query_sets AS qs
                       WHERE qs.name = %s;""" % wrap_value(name)

        group_query = """SELECT q.id, g.id, nq.node_id, qc.estimate
                         FROM node_queries AS nq
                         JOIN queries AS g ON g.id = nq.query_id
                         JOIN queries AS q ON q.id = g.subquery_of_id
                         JOIN query_sets AS qs ON qs.id = q.set_id
                         JOIN query_cardinalities AS qc ON qc.query_id = g.id
                         JOIN cardinality_estimators AS ce ON ce.id = qc.cardinality_estimator_id
                         WHERE qs.name = %s AND q.subquery_of_id IS NULL AND ce.name = %s;""" % (wrap_value(name), wrap_value(query_cardinality_estimator))

        cursor = self._connection.cursor()
        cursor.execute(set_query)
        set_id, database_id = cursor.fetchone()

        cursor.execute(group_query)

        groups = {}
        cardinalities = {}
        for query_id, group_id, node_id, cardinality in cursor.fetchall():
            if query_id not in groups:
                groups[query_id] = {}
            if group_id not in groups[query_id]:
                groups[query_id][group_id] = []
                cardinalities[group_id] = cardinality
            groups[query_id][group_id].append(nodes[node_id])

        cursor.close()

        node_dict = {}
        for node_id in nodes:
            node_dict[nodes[node_id]] = node_id

        annotated_queries = []
        query_dict = {}
        group_cardinalities = {}
        group_ids = {}
        for query_id in queries:
            if query_id not in groups and only_annotated:
                continue
            query_dict[queries[query_id]] = query_id
            annotated_queries.append(queries[query_id])
            group_cardinalities[queries[query_id]] = {}
            group_ids[queries[query_id]] = {}
            if query_id not in groups:
                continue
            for group_id in groups[query_id]:
                group = frozenset(groups[query_id][group_id])
                group_cardinalities[queries[query_id]][group] = cardinalities[group_id]
                if all_group_ids:
                    if group not in group_ids[queries[query_id]]:
                        group_ids[queries[query_id]][group] = []
                    group_ids[queries[query_id]][group].append(group_id)
                else:
                    group_ids[queries[query_id]][group] = group_id
        return annotated_queries, query_dict, node_dict, group_cardinalities, set_id, database_id, query_names, group_ids

    def _load_nodes(self,
                    set_id: int,
                    labels: Dict[int, List[SchemaNode]],
                    predicates: Dict[int, List[Predicate]],
                    language: str,
                    estimator_id: int
                    ) -> Tuple[Dict[int, List[SchemaNode]], Dict[int, SchemaNode]]:
        cursor = self._connection.cursor()

        node_query = """SELECT q.id, n.id, n.virtual, nc.estimate
                        FROM nodes AS n
                        JOIN node_queries AS nq ON nq.node_id = n.id
                        JOIN queries AS q ON q.id = nq.query_id
                        LEFT OUTER JOIN node_cardinalities AS nc ON nc.node_id = n.id AND cardinality_estimator_id = %d
                        WHERE q.set_id = %d AND q.subquery_of_id IS NULL;""" % (estimator_id, set_id)
        cursor.execute(node_query)
        nodes = {}
        query_nodes = {}
        for query_id, node_id, virtual, estimate in cursor.fetchall():
            node_labels = []
            if node_id in labels:
                node_labels = labels[node_id]
            node_predicates = []
            if node_id in predicates:
                node_predicates = predicates[node_id]
            if language == "sql":
                assert(len(node_labels) == 1)
                nodes[node_id] = SQLTableInstance(node_labels[0], estimate, node_predicates, virtual=virtual)
            else:
                raise NotImplementedError()
            if query_id not in query_nodes:
                query_nodes[query_id] = []
            query_nodes[query_id].append(nodes[node_id])
        return query_nodes, nodes

    def _load_nodes_query(self,
                          query_id: int,
                          labels: Dict[int, List[SchemaNode]],
                          predicates: Dict[int, List[Predicate]],
                          language: str,
                          estimator_id: int
                          ) -> Dict[int, SchemaNode]:
        cursor = self._connection.cursor()

        node_query = """SELECT n.id, n.virtual, nc.estimate
                        FROM nodes AS n
                        JOIN node_queries AS nq ON nq.node_id = n.id
                        JOIN queries AS q ON q.id = nq.query_id
                        LEFT OUTER JOIN node_cardinalities AS nc ON nc.node_id = n.id AND cardinality_estimator_id = %d
                        WHERE q.id = %d;""" % (estimator_id, query_id)
        cursor.execute(node_query)
        nodes = {}
        for node_id, virtual, estimate in cursor.fetchall():
            node_labels = []
            if node_id in labels:
                node_labels = labels[node_id]
            node_predicates = []
            if node_id in predicates:
                node_predicates = predicates[node_id]
            if language == "sql":
                assert(len(node_labels) == 1)
                nodes[node_id] = SQLTableInstance(node_labels[0], estimate, node_predicates, virtual=virtual)
            else:
                raise NotImplementedError()
        return nodes

    def _load_edges(self,
                    set_id: int,
                    nodes: Dict[int, QueryNode],
                    labels: Dict[int, List[SchemaEdge]],
                    predicates: Dict[int, List[Predicate]],
                    language: str) -> Dict[int, List[Tuple[QueryNode, QueryEdge, QueryNode]]]:
        cursor = self._connection.cursor()

        edge_query = """SELECT q.id, e.from_id, e.id, e.to_id, d.name
                        FROM edges AS e
                        JOIN nodes AS n ON n.id = e.from_id
                        JOIN node_queries AS nq ON nq.node_id = n.id
                        JOIN queries AS q ON q.id = nq.query_id
                        JOIN directions AS d ON d.id = e.direction_id
                        WHERE q.set_id = %d AND q.subquery_of_id IS NULL;""" % set_id

        cursor.execute(edge_query)

        edges = {}
        for query_id, from_id, edge_id, to_id, direction_name in cursor.fetchall():
            edge_labels = []
            if edge_id in labels:
                edge_labels = labels[edge_id]
            edge_predictes = []
            if edge_id in predicates:
                edge_predictes = predicates[edge_id]
            direction = EdgeDirection.from_string(direction_name)
            if language == "sql":
                assert(len(edge_labels) == 1)
                edge = SQLJoin(edge_labels[0])
            else:
                raise NotImplementedError()
            if query_id not in edges:
                edges[query_id] = []
            edges[query_id].append((nodes[from_id], edge, nodes[to_id]))

        cursor.close()

        return edges

    def _load_edges_query(self,
                          query_id: int,
                          nodes: Dict[int, QueryNode],
                          labels: Dict[int, List[SchemaEdge]],
                          predicates: Dict[int, List[Predicate]],
                          language: str) -> Dict[int, List[Tuple[QueryNode, QueryEdge, QueryNode]]]:
        cursor = self._connection.cursor()

        edge_query = """SELECT q.id, e.from_id, e.id, e.to_id, d.name
                        FROM edges AS e
                        JOIN nodes AS n1 ON n1.id = e.from_id
                        JOIN nodes AS n2 ON n2.id = e.to_id
                        JOIN node_queries AS nq1 ON nq1.node_id = n1.id
                        JOIN node_queries AS nq2 ON nq2.node_id = n2.id
                        JOIN queries AS q ON q.id = nq1.query_id AND q.id = nq2.query_id
                        JOIN directions AS d ON d.id = e.direction_id
                        WHERE q.id = %d;""" % query_id

        cursor.execute(edge_query)

        edges = []
        for query_id, from_id, edge_id, to_id, direction_name in cursor.fetchall():
            edge_labels = []
            if edge_id in labels:
                edge_labels = labels[edge_id]
            edge_predictes = []
            if edge_id in predicates:
                edge_predictes = predicates[edge_id]
            direction = EdgeDirection.from_string(direction_name)
            if language == "sql":
                assert(len(edge_labels) == 1)
                edge = SQLJoin(edge_labels[0])
            else:
                raise NotImplementedError()
            edges.append((nodes[from_id], edge, nodes[to_id]))

        cursor.close()

        return edges

    def _load_node_labels(self, schema: GraphlikeSchema, set_id: int) -> Dict[int, List[SchemaNode]]:
        label_query = """SELECT n.id, l.id, l.name
                         FROM labels AS l
                         JOIN element_labels AS el ON el.label_id = l.id
                         JOIN nodes AS n ON n.id = el.element_id
                         JOIN node_queries AS nq ON nq.node_id = n.id
                         JOIN queries AS q ON q.id = nq.query_id
                         WHERE q.set_id = %d AND q.subquery_of_id IS NULL;""" % set_id
        cursor = self._connection.cursor()
        cursor.execute(label_query)

        label_dict: Dict[int, SchemaNode] = {}
        labels: Dict[int, List[SchemaNode]] = {}
        for element_id, label_id, label_name in cursor.fetchall():
            if label_id not in label_dict:
                label_dict[label_id] = schema.node(label_name)

            if element_id not in labels:
                labels[element_id] = []
            labels[element_id].append(label_dict[label_id])

        cursor.close()

        return labels

    def _load_node_labels_query(self, schema: GraphlikeSchema, query_id: int) -> Dict[int, List[SchemaNode]]:
        label_query = """SELECT n.id, l.id, l.name
                         FROM labels AS l
                         JOIN element_labels AS el ON el.label_id = l.id
                         JOIN nodes AS n ON n.id = el.element_id
                         JOIN node_queries AS nq ON nq.node_id = n.id
                         JOIN queries AS q ON q.id = nq.query_id
                         WHERE q.id = %d;""" % query_id
        cursor = self._connection.cursor()
        cursor.execute(label_query)

        label_dict: Dict[int, SchemaNode] = {}
        labels: Dict[int, List[SchemaNode]] = {}
        for element_id, label_id, label_name in cursor.fetchall():
            if label_id not in label_dict:
                label_dict[label_id] = schema.node(label_name)

            if element_id not in labels:
                labels[element_id] = []
            labels[element_id].append(label_dict[label_id])

        cursor.close()

        return labels

    def _load_edge_labels(self, schema: GraphlikeSchema, set_id: int) -> Dict[int, List[SchemaEdge]]:
        label_query = """SELECT e.id, l.id, l.name
                         FROM labels AS l
                         JOIN element_labels AS el ON el.label_id = l.id
                         JOIN edges AS e ON e.id = el.element_id
                         JOIN nodes AS n ON n.id = e.from_id
                         JOIN node_queries AS nq ON nq.node_id = n.id
                         JOIN queries AS q ON q.id = nq.query_id
                         WHERE q.set_id = %d AND q.subquery_of_id IS NULL;""" % set_id
        cursor = self._connection.cursor()
        cursor.execute(label_query)

        label_dict: Dict[int, SchemaEdge] = {}
        labels: Dict[int, List[SchemaEdge]] = {}
        for element_id, label_id, label_name in cursor.fetchall():
            if label_id not in label_dict:
                label_dict[label_id] = schema.edge(label_name)

            if element_id not in labels:
                labels[element_id] = []
            labels[element_id].append(label_dict[label_id])

        cursor.close()

        return labels

    def _load_edge_labels_query(self, schema: GraphlikeSchema, query_id: int) -> Dict[int, List[SchemaEdge]]:
        label_query = """SELECT e.id, l.id, l.name
                         FROM labels AS l
                         JOIN element_labels AS el ON el.label_id = l.id
                         JOIN edges AS e ON e.id = el.element_id
                         JOIN nodes AS n ON n.id = e.from_id
                         JOIN node_queries AS nq ON nq.node_id = n.id
                         JOIN queries AS q ON q.id = nq.query_id
                         WHERE q.id = %d;""" % query_id
        cursor = self._connection.cursor()
        cursor.execute(label_query)

        label_dict: Dict[int, SchemaEdge] = {}
        labels: Dict[int, List[SchemaEdge]] = {}
        for element_id, label_id, label_name in cursor.fetchall():
            if label_id not in label_dict:
                label_dict[label_id] = schema.edge(label_name)

            if element_id not in labels:
                labels[element_id] = []
            labels[element_id].append(label_dict[label_id])

        cursor.close()

        return labels

    def _load_node_predicates(self,
                              node_labels: Dict[int, List[SchemaNode]],
                              set_id: int) -> Dict[int, List[List[Predicate]]]:
        node_predicate_query = """SELECT n.id, d.id, p.attribute_name, co.name, p.value, p.positive
                                  FROM predicates AS p
                                  JOIN disjunctions AS d ON d.id = p.disjunction_id
                                  JOIN nodes AS n ON n.id = d.element_id
                                  JOIN node_queries AS qn ON qn.node_id = n.id
                                  JOIN queries AS q ON q.id = qn.query_id
                                  JOIN comparison_operators AS co ON co.id = p.operator_id
                                  WHERE q.set_id = %d AND q.subquery_of_id IS NULL;""" % set_id
        cursor = self._connection.cursor()
        cursor.execute(node_predicate_query)
        predicate_dict = self._build_predicates(node_labels, cursor.fetchall())
        cursor.close()
        return predicate_dict

    def _load_node_predicates_query(self,
                                    node_labels: Dict[int, List[SchemaNode]],
                                    query_id: int) -> Dict[int, List[List[Predicate]]]:
        node_predicate_query = """SELECT n.id, d.id, p.attribute_name, co.name, p.value, p.positive
                                  FROM predicates AS p
                                  JOIN disjunctions AS d ON d.id = p.disjunction_id
                                  JOIN nodes AS n ON n.id = d.element_id
                                  JOIN node_queries AS qn ON qn.node_id = n.id
                                  JOIN queries AS q ON q.id = qn.query_id
                                  JOIN comparison_operators AS co ON co.id = p.operator_id
                                  WHERE q.id = %d;""" % query_id
        cursor = self._connection.cursor()
        cursor.execute(node_predicate_query)
        predicate_dict = self._build_predicates(node_labels, cursor.fetchall())
        cursor.close()
        return predicate_dict

    def _load_edge_predicates(self,
                              edge_labels: Dict[int, List[SchemaNode]],
                              set_id: int) -> Dict[int, List[List[Predicate]]]:
        edge_predicate_query = """SELECT e.id, d.id, p.attribute_name, co.name, p.value, p.positive
                                  FROM predicates AS p
                                  JOIN disjunctions AS d ON d.id = p.disjunction_id
                                  JOIN edges AS e ON e.id = d.element_id
                                  JOIN nodes AS n ON n.id = e.from_id
                                  JOIN node_queries AS qn ON qn.node_id = n.id
                                  JOIN queries AS q ON q.id = qn.query_id
                                  JOIN comparison_operators AS co ON co.id = p.operator_id
                                  WHERE q.set_id = %d AND q.subquery_of_id IS NULL;""" % set_id
        cursor = self._connection.cursor()
        cursor.execute(edge_predicate_query)
        predicate_dict = self._build_predicates(edge_labels, cursor.fetchall())
        cursor.close()
        return predicate_dict

    def _load_edge_predicates_query(self,
                                    edge_labels: Dict[int, List[SchemaNode]],
                                    query_id: int) -> Dict[int, List[List[Predicate]]]:
        edge_predicate_query = """SELECT e.id, d.id, p.attribute_name, co.name, p.value, p.positive
                                  FROM predicates AS p
                                  JOIN disjunctions AS d ON d.id = p.disjunction_id
                                  JOIN edges AS e ON e.id = d.element_id
                                  JOIN nodes AS n ON n.id = e.from_id
                                  JOIN node_queries AS qn ON qn.node_id = n.id
                                  JOIN queries AS q ON q.id = qn.query_id
                                  JOIN comparison_operators AS co ON co.id = p.operator_id
                                  WHERE q.id = %d;""" % query_id
        cursor = self._connection.cursor()
        cursor.execute(edge_predicate_query)
        predicate_dict = self._build_predicates(edge_labels, cursor.fetchall())
        cursor.close()
        return predicate_dict

    def _build_predicates(self,
                          labels: Dict[int, List[Union[SchemaNode, SchemaEdge]]],
                          iterator: Iterator[Tuple[int, int, str, str, Any]]) -> Dict[int, List[List[Predicate]]]:
        elements = {}
        for element_id, disjunction_id, attribute_name, operator_symbol, value, positive in iterator:
            if element_id not in elements:
                elements[element_id] = {}
            if disjunction_id not in elements[element_id]:
                elements[element_id][disjunction_id] = []
            attribute = None
            for label in labels[element_id]:
                if isinstance(label, Attributable) and label.attribute_exists(attribute_name):
                    attribute = label.attribute(attribute_name)
                    break
            assert(attribute is not None)
            operator = OPERATORS[operator_symbol]
            if value is not None:
                python_type = attribute.data_type().python_type()
                if python_type == int:
                    value = int(value)
                elif python_type == float:
                    value = float(value)
                elif python_type == datetime.date:
                    value = datetime.datetime.strptime(value, "%Y-%m-%d")
                elif python_type == datetime.datetime:
                    value = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                elif python_type != str:
                    raise NotImplementedError()

            predicate = Predicate(attribute, operator, value, positive=positive)
            elements[element_id][disjunction_id].append(predicate)
        for element_id in elements:
            disjunctions = []
            for disjunction_id in elements[element_id]:
                disjunctions.append(elements[element_id][disjunction_id])
            elements[element_id] = disjunctions
        return elements

    def _load_cardinalities(self, set_id: int, all: bool) -> Dict[Tuple[int, str], int]:
        if all:
            condition = ""
        else:
            condition = " AND qc.cardinality_estimator_id = 1"
        label_query = """SELECT q.id, ce.name, qc.estimate
                         FROM queries AS q
                         JOIN query_cardinalities AS qc ON qc.query_id = q.id
                         JOIN cardinality_estimators AS ce ON ce.id = qc.cardinality_estimator_id
                         WHERE q.set_id = %d AND q.subquery_of_id IS NULL%s;""" % (set_id, condition)
        cursor = self._connection.cursor()
        cursor.execute(label_query)

        cardinalities = {}
        for query_id, estimator, cardinality in cursor.fetchall():
            cardinalities[(query_id, estimator)] = cardinality

        cursor.close()

        return cardinalities


    def _load_cardinalities_query(self, query_id: int, all: bool) -> Dict[str, int]:
        if all:
            condition = ""
        else:
            condition = " AND qc.cardinality_estimator_id = 1"
        label_query = """SELECT ce.name, qc.estimate
                         FROM queries AS q
                         JOIN query_cardinalities AS qc ON qc.query_id = q.id
                         JOIN cardinality_estimators AS ce ON ce.id = qc.cardinality_estimator_id
                         WHERE q.id = %d%s;""" % (query_id, condition)
        cursor = self._connection.cursor()
        cursor.execute(label_query)

        cardinalities = {}
        for estimator, cardinality in cursor.fetchall():
            cardinalities[estimator] = cardinality

        cursor.close()

        return cardinalities

    def load_cardinities_ids(self, ids: List[int], name: str = "true") -> List[Tuple[int, int]]:
        cardinality_query = """SELECT qc.query_id, qc.estimate
                               FROM query_cardinalities AS qc
                               JOIN cardinality_estimators AS ce ON ce.id = qc.cardinality_estimator_id
                               WHERE ce.name = '%s' AND qc.query_id IN (%s);""" % (name, ",".join([str(qid) for qid in ids]))
        cursor = self._connection.cursor()
        cursor.execute(cardinality_query)
        result = list(cursor.fetchall())
        cursor.close()
        return result

    def load_superquery_ids(self, ids: List[int]) -> List[Tuple[int, int]]:
        superquery_query = """SELECT q.id, q.subquery_of_id
                              FROM queries AS q
                              WHERE q.id IN (%s);""" % ",".join([str(qid) for qid in ids])
        cursor = self._connection.cursor()
        cursor.execute(superquery_query)
        result = list(cursor.fetchall())
        cursor.close()
        return result

    def save_queries(self,
                     queries: List[GraphlikeQuery],
                     name: str,
                     database_name: str,
                     estimator_id: int,
                     latencies: Dict[GraphlikeQuery, Dict[str, float]]
                     ) -> Tuple[int, int, List[Tuple[int, Dict[QueryNode, int]]]]:
        database_id = self._get("databases", {"name": database_name})
        set_id = self._get_or_create("query_sets", {"name": name, "database_id": database_id})
        query_ids = []
        for i, query in enumerate(queries):
            if query in latencies:
                query_ids.append(self.save_query(query, database_id, set_id, estimator_id, latencies=latencies[query]))
            else:
                query_ids.append(self.save_query(query, database_id, set_id, estimator_id))
            print(i)
        self._connection.commit()
        return database_id, set_id, query_ids

    def save_query(self,
                   query: GraphlikeQuery,
                   database_id: int,
                   set_id: int,
                   estimator_id: int,
                   latencies: Dict[str, float] = {}
                   ) -> Tuple[int, Dict[QueryNode, int]]:
        if query.name() is None:
            query_id = self._create("queries", {"set_id": set_id})
        else:
            query_id = self._create("queries", {"set_id": set_id, "name": query.name()})

        node_ids = {}
        for node in query.nodes():
            node_ids[node] = self._save_node(node, database_id, query_id, estimator_id)

        for from_node, edge, to_node in query.edges():
            self._save_edge(edge, node_ids[from_node], node_ids[to_node], database_id)

        cardinality_estimates = query.cardinality_estimates()
        for estimator_name in query.cardinality_estimates():
            estimator_id = self._get_or_create("cardinality_estimators", {"name": estimator_name})
            estimation_attributes = {}
            estimation_attributes["query_id"] = query_id
            estimation_attributes["cardinality_estimator_id"] = estimator_id
            estimation_attributes["estimate"] = cardinality_estimates[estimator_name]
            if estimator_name in latencies:
                estimation_attributes["estimation_latency"] = latencies["estimator_name"]
            self._create("query_cardinalities", estimation_attributes, return_id=False)

        return query_id, node_ids

    def save_group(self, query_id: int, node_ids: Dict[QueryNode, int], group: GraphlikeQuery, set_id: int, cardinality: int):
        group_id = self._create("queries", {"set_id": set_id, "subquery_of_id": query_id})
        for node in group.nodes():
            if not node.virtual():
                self._create("node_queries", {"node_id": node_ids[node], "query_id": group_id}, return_id=False)
        self._create("query_cardinalities", {"query_id": group_id, "cardinality_estimator_id": 1, "estimate": cardinality}, return_id=False)

    def save_cardinality_estimation(self, query_id: int, estimation: float, estimator_name: str):
        estimator_id = self._get("cardinality_estimators", {"name": estimator_name})
        self._create("query_cardinalities", {"query_id": query_id, "cardinality_estimator_id": estimator_id, "estimate": estimation}, return_id=False)

    def _save_node(self, node: QueryNode, database_id: int, query_id: int, estimator_id: int) -> int:
        label_ids = []

        node_id = self._create("elements", {})
        self._create("nodes", {"id": node_id, "virtual": node.virtual()})
        self._create("node_queries", {"node_id": node_id, "query_id": query_id}, return_id=False)

        for label in node.labels():
            label_ids.append(self._save_label(label, database_id))

        for label_id in label_ids:
            self._create("element_labels", {"element_id": node_id, "label_id": label_id}, return_id=False)

        if isinstance(node, Predicatable):
            self._save_predicatable(node, node_id)

        node_cardinality_attributes = {}
        node_cardinality_attributes["node_id"] = node_id
        node_cardinality_attributes["cardinality_estimator_id"] = estimator_id
        node_cardinality_attributes["estimate"] = node.cardinality()
        self._create("node_cardinalities", node_cardinality_attributes, return_id=False)
        return node_id

    def _save_edge(self, edge: QueryEdge, from_id: int, to_id: int, database_id: int) -> int:
        label_ids = []

        direction_id = self._directions[EdgeDirection.string(edge.direction())]

        edge_id = self._create("elements", {})
        self._create("edges", {"id": edge_id, "direction_id": direction_id, "from_id": from_id, "to_id": to_id})

        for label in edge.labels():
            label_ids.append(self._save_label(label, database_id))

        if isinstance(edge, Predicatable):
            self._save_predicatable(edge, edge_id)

        for label_id in label_ids:
            self._create("element_labels", {"element_id": edge_id, "label_id": label_id}, return_id=False)

        return edge_id

    def _save_label(self, label: Union[SchemaNode, SchemaEdge], database_id: int) -> int:
        attributes = {}
        attributes["database_id"] = database_id
        if isinstance(label, SchemaNode):
            attributes["is_node_label"] = "TRUE"
        else:
            attributes["is_node_label"] = "FALSE"
        attributes["name"] = label.name()
        return self._get_or_create("labels", attributes)

    def _save_predicatable(self, element: Predicatable, element_id: int):
        for disjunction in element.predicates():
            disjunction_id = self._create("disjunctions", {"element_id": element_id})
            for predicate in disjunction:
                self._save_predicate(predicate, disjunction_id)

    def _save_predicate(self, predicate: Predicate, disjunction_id: int) -> int:
        attributes = {}
        attributes["disjunction_id"] = disjunction_id
        attributes["attribute_name"] = predicate.attribute().name()
        attributes["operator_id"] = self._operators[predicate.operator().symbol()]
        if predicate.value() is not None:
            attributes["value"] = predicate.value()
        attributes["positive"] = predicate.positive()
        return self._create("predicates", attributes)

    def _get(self, table: str, attributes: Dict[str, Any]) -> Optional[int]:
        assert(len(attributes) > 0)
        cursor = self._connection.cursor()
        attribute_strings = [attribute + " = " + wrap_value(attributes[attribute]) for attribute in attributes]
        query = "SELECT id FROM %s WHERE %s;" % (table, " AND ".join(attribute_strings))
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        if result is None:
            return None
        return result[0]

    def _create(self, table: str, attributes: Dict[str, Any], return_id: bool = True) -> Optional[int]:
        cursor = self._connection.cursor()
        key_strings = attributes.keys()
        value_strings = [wrap_value(attributes[attribute]) for attribute in attributes]
        if len(attributes) == 0:
            query = "INSERT INTO %s DEFAULT VALUES" % table
        else:
            query = "INSERT INTO %s (%s) VALUES (%s)" % (table, ", ".join(key_strings), ", ".join(value_strings))
        if return_id:
            query += " RETURNING id;"
        else:
            query += ";"
        cursor.execute(query)
        result = None
        if return_id:
            result = cursor.fetchone()[0]
        cursor.close()
        return result

    def _get_or_create(self, table: str, attributes: Dict[str, Any]) -> int:
        get = self._get(table, attributes)
        if get is None:
            return self._create(table, attributes)
        return get

    def update_node_cardinality(self, node_id: int, estimator_id: int, estimate: float):
        update_query = """UPDATE node_cardinalities
                          SET estimate = %f
                          WHERE node_id = %d AND cardinality_estimator_id = %d;""" % (estimate, node_id, estimator_id)
        cursor = self._connection.cursor()
        cursor.execute(update_query)
        cursor.close()
        self.commit()

    def update_query_cardinality(self, query_id: int, estimator_id: int, estimate: float):
        update_query = """UPDATE query_cardinalities
                          SET estimate = %f
                          WHERE query_id = %d AND cardinality_estimator_id = %d;""" % (estimate, query_id, estimator_id)
        cursor = self._connection.cursor()
        cursor.execute(update_query)
        cursor.close()
        self.commit()
