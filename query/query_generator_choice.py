

import random
from typing import List
from query.graphlike_query import GraphlikeQuery
from query.query_generator import QueryGenerator


class QueryGeneratorChoice(QueryGenerator):
    def __init__(self, queries: List[GraphlikeQuery]) -> None:
        super().__init__()
        self._queries = queries

    def generate_query_default(self) -> GraphlikeQuery:
        return random.choice(self._queries)
