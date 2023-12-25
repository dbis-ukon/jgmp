
import abc

from query.graphlike_query import GraphlikeQuery


class QueryGenerator:
    @abc.abstractmethod
    def generate_query_default(self) -> GraphlikeQuery:
        pass
