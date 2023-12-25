import torch

from cardinality_estimator.learned_cardinality_estimator import LearnedCardinalityEstimator
from cardinality_estimator.prophetic_cardinality_estimator import PropheticCardinalityEstimator
from data.query_db import QueryDB
from encoder.encoder import Encoder
from encoder.sql_sampler import SQLSampler
from schema.graphlike_schema import GraphlikeSchema
from query.sql.sql_query import SQLQuery
from typing import List, Tuple, Any, Optional
from schema.sql.sql_schema import SQLSchema


def nonempty_powerset(s: List[Any]) -> List[List[Any]]:
    ps = []
    for i in range(1 << len(s)):
        subset = [s[j] for j in range(len(s)) if (i & (1 << j))]
        if len(subset) > 0:
            ps.append(subset)
    return ps


def load_learned_estimator_sql(schema: SQLSchema, model_path: str, model_config, device: Optional[Any], attribute_table_order: Optional[List[str]] = None) -> LearnedCardinalityEstimator:
    sampler = SQLSampler.load(schema, model_path + ".samples")
    if "eliminate_lesser" in model_config:
        encoder = Encoder(schema, sampler, eliminate_lesser=model_config["eliminate_lesser"], attribute_table_order=attribute_table_order)
    else:
        encoder = Encoder(schema, sampler, attribute_table_order=attribute_table_order)
    model = model_config["type"].from_config(encoder, model_config)
    model.load_state_dict(torch.load(model_path + ".pt"))
    if device is not None:
        model.to(device)
    return LearnedCardinalityEstimator(model, device=device)


def load_prophetic_estimator(schema: GraphlikeSchema, query_db: QueryDB, query_set_name: str, competitor: str) -> Tuple[PropheticCardinalityEstimator, List[SQLQuery]]:
    queries, _, _, group_cardinalities, _, _, _, _ = query_db.load_group_cardinalities(schema, query_set_name, 2, True, query_cardinality_estimator=competitor)
    return PropheticCardinalityEstimator(schema, group_cardinalities), queries
