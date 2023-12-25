from typing import Optional

from schema.sql.sql_schema import SQLSchema


def imdb_schema(port: Optional[int] = None) -> SQLSchema:
    return SQLSchema.sql_schema_from_connection("imdb_ceb", leafs=["comp_cast_type", "company_type", "info_type", "kind_type", "link_type"], fk_name="imdb_schema", port=port)

def imdb_light_schema(port: Optional[int] = None) -> SQLSchema:
    schema = SQLSchema.sql_schema_from_connection("imdb_ceb",
                                                  leafs=["movie_info", "movie_info_idx", "movie_companies", "movie_keyword", "cast_info"],
                                                  mask={"title", "movie_info", "movie_info_idx", "movie_companies", "movie_keyword", "cast_info"},
                                                  fk_name="imdb_schema",
                                                  port=port)
    return schema

def stats_schema(port: Optional[int] = None) -> SQLSchema:
    return SQLSchema.sql_schema_from_connection("stats-ceb", port=port)

def stack_schema(port: Optional[int] = None) -> SQLSchema:
    return SQLSchema.sql_schema_from_connection("stack", port=port)

def dsb_schema(port: Optional[int] = None) -> SQLSchema:
    return SQLSchema.sql_schema_from_connection("dsb", fk_name="dsb_schema", port=port)
