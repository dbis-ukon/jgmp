
from query.query_generator import QueryGenerator
from query.query_generator_schema import QueryGeneratorSchema
from query.sql.sql_query import SQLQuery
from schema.attributable import Attributable
from schema.attribute import Attribute
from schema.sql.sql_foreign_key import SQLForeignKey
from schema.sql.sql_random_attribute_buffer import SQLRandomAttributeBuffer
from schema.sql.sql_schema import SQLSchema
from schema.sql.sql_table import SQLTable
from typing import List, Optional, Tuple
from query.sql.sql_join import SQLJoin
from query.sql.sql_table_instance import SQLTableInstance
from schema.value_picker import ValuePicker


class SQLQueryGeneratorSchema(QueryGeneratorSchema):
    def __init__(self,
                 schema: SQLSchema,
                 seed: Optional[int] = 42,
                 value_picker: Optional[ValuePicker] = None,
                 default_min_nodes: int = 2,
                 default_max_nodes: int = 17,
                 default_predicate_factor: float = 0.3) -> None:
        if value_picker is None:
            value_picker = SQLRandomAttributeBuffer(schema)
        super().__init__(schema, value_picker, default_min_nodes, default_max_nodes, default_predicate_factor, seed=seed)
        self._predicate_columns = {}
        for node in schema.nodes():
            self._predicate_columns[node] = set()
            assert(isinstance(node, Attributable) and isinstance(node, SQLTable))
            for column in node.attributes():
                if not node.is_key(column):
                    self._predicate_columns[node].add(column)

        for node in schema.nodes():
            for edge, to_node in schema.edges_from(node):
                assert(isinstance(edge, SQLForeignKey))
                for column in edge.foreign_key_attributes():
                    self._predicate_columns[node].remove(column)
                for column in edge.primary_key_attributes():
                    self._predicate_columns[to_node].remove(column)

    def generate_query(self,
                       min_nodes: int,
                       max_nodes: int,
                       predicate_factor: float) -> SQLQuery:
        nodes = None
        while nodes is None or len(nodes) < min_nodes or len(nodes) > max_nodes:
            nodes, edges = self._generate_nodes_and_edges(min_nodes, max_nodes, predicate_factor, True)
        return SQLQuery(nodes, edges)

    def _possible_predicates(self, entity: Attributable) -> List[Attribute]:
        return self._predicate_columns[entity]

    def _generate_node(self, schema_node: SQLTable, num_predicates: int) -> Tuple[SQLTableInstance, int]:
        predicates = self._generate_predicates(schema_node, num_predicates)
        return SQLTableInstance.build(self._schema, schema_node, predicates)

    def _generate_edge(self, schema_edge: SQLForeignKey, num_predicates: int) -> Tuple[SQLJoin, int]:
        return SQLJoin(schema_edge)
