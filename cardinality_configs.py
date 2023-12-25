from typing import List, Dict, Any

from models.bulk_jgmp_cardinality_model import BulkJGMPCardinalityModel
from models.bulk_mscn_cardinality_model import BulkMSCNCardinalityModel
from models.graph_cardinality_model import GraphCardinalityModel
from models.util import AggregationType



mscn_config = {'name': "mscn",
               'type': BulkMSCNCardinalityModel,
               'batch_size': 32,
               'bitmap_size': 0,
               'eliminate_lesser': False,
               'epochs': 300,
               'hidden_units': 128,
               'activation': "relu",
               'aggregation': "mean",
               'include_predicates': True,
               'independent_samples': False,
               'independent_sample_size': None,
               'learning_rate': 0.0005,
               'samples_random': True,
               'samples_separate': True,
               'selfsupervised_factor': 1}


mscn_hybrid_config = {'name': "mscn_hybrid",
                      'type': BulkMSCNCardinalityModel,
                      'batch_size': 32,
                      'bitmap_size': 16,
                      'eliminate_lesser': False,
                      'epochs': 300,
                      'hidden_units': 128,
                      'activation': "leakyrelu",
                      'aggregation': "multihead",
                      'include_predicates': False,
                      'independent_samples': True,
                      'independent_sample_size': 32,
                      'learning_rate': 0.0005,
                      'samples_random': False,
                      'samples_separate': True,
                      'min_gain': 0,
                      'selfsupervised_factor': 1}


jgmp_config = {'name': "jgmp",
               'type': BulkJGMPCardinalityModel,
               'sample_layer_size': 32,
               'node_size': 64,
               'node_number': 0,
               'edge_size': 8,
               'edge_number': 1,
               'size_per_tower': 8,
               'towers': 4,
               'conv_number': 3,
               'skip_connections': True,
               'node_aggregate_head_size': 128,
               'node_aggregate_heads': 8,
               'final_size': 256,
               'final_number': 1,
               'samples_random': False,
               'samples_separate': True,
               'min_gain': 0,
               'bitmap_size': 16,
               'use_pg_estimates': True,
               'encode_fk_direction': True,
               'epochs': 300,
               'learning_rate': 0.0005,
               'batch_size': 32,
               'eliminate_lesser': False,
               'selfsupervised_factor': 1}


def semisupervised_experiment_configs(base_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    supervised_config = dict(base_config)
    supervised_config["name"] += "_supervised"
    supervised_config["selfsupervised_factor"] = 0
    semisupervised_config = dict(base_config)
    semisupervised_config["name"] += "_semisupervised"
    semisupervised_config["selfsupervised_factor"] = 1
    return [supervised_config, semisupervised_config]


def selfsupervised_factor_experiment_configs(base_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    supervised_config = dict(base_config)
    supervised_config["name"] += "_supervised"
    supervised_config["selfsupervised_factor"] = 0

    configs = [supervised_config]
    for selfsupervised_factor in [0.1, 0.3, 1, 3, 10]:
        semisupervised_config = dict(base_config)
        semisupervised_config["name"] += "_semisupervised%f" % selfsupervised_factor
        semisupervised_config["selfsupervised_factor"] = selfsupervised_factor
        configs.append(semisupervised_config)

    return configs


def default_configs() -> List[Dict[str, Any]]:
    return semisupervised_experiment_configs(mscn_config) + semisupervised_experiment_configs(mscn_hybrid_config) + semisupervised_experiment_configs(jgmp_config)


def fs_configs() -> List[Dict[str, Any]]:
    return semisupervised_experiment_configs(mscn_hybrid_config) + semisupervised_experiment_configs(jgmp_config)


def feature_selection_ablation_configs() -> List[Dict[str, Any]]:
    no_bitmap_config = dict(jgmp_config)
    no_bitmap_config["name"] += "_no_bitmap"
    no_bitmap_config["bitmap_size"] = 0
    no_bitmap_config["samples_random"] = True

    random_bitmap_config = dict(jgmp_config)
    random_bitmap_config["name"] += "_random_bitmap"
    random_bitmap_config["bitmap_size"] = 16
    random_bitmap_config["samples_random"] = True

    thousand_random_bitmap_config = dict(jgmp_config)
    thousand_random_bitmap_config["name"] += "_1000_random_samples"
    thousand_random_bitmap_config["bitmap_size"] = 1000
    thousand_random_bitmap_config["samples_random"] = True

    return [no_bitmap_config, random_bitmap_config, thousand_random_bitmap_config]


def fk_direction_ablation_configs() -> List[Dict[str, Any]]:
    no_pg_config = dict(jgmp_config)
    no_pg_config["name"] += "_no_pg"
    no_pg_config["use_pg_estimates"] = False

    no_fk_dir_config = dict(jgmp_config)
    no_fk_dir_config["name"] += "_no_fk_dir"
    no_fk_dir_config["encode_fk_direction"] = False

    no_pg_no_fk_dir_config = dict(jgmp_config)
    no_pg_no_fk_dir_config["name"] += "_no_pg_no_fk_dir"
    no_pg_no_fk_dir_config["use_pg_estimates"] = False
    no_pg_no_fk_dir_config["encode_fk_direction"] = False

    return [no_pg_config, no_fk_dir_config, no_pg_no_fk_dir_config]


def incremental_configs(configs: List[Dict[str, Any]], increments: List[int]) -> List[Dict[str, Any]]:
    increment_configs = []
    for increment in increments:
        for config in configs:
            increment_config = dict(config)
            increment_config["name"] += "_%d" % increment
            increment_configs.append(increment_config)
    return increment_configs


def deprecated_config_names(config: Dict[str, Any]):
    config_name = config["name"]
    if config_name == "mscn":
        config["name"] = "mscn_flowloss_bulk"
    elif config_name == "mscn_hybrid":
        config["name"] = "mscn_improved_bulk"
    elif config_name == "jgmp":
        config["name"] = "bulk_light_graph"


def redo_incremental_configs() -> List[Dict[str, Any]]:
    new_configs = [mscn_config, mscn_hybrid_config, jgmp_config]
    configs = []
    for config in new_configs:
        deprecated_config_names(config)
        configs += semisupervised_experiment_configs(config)
    configs = incremental_configs(configs, [128, 256, 512, 1024, 2048])
    return configs

