

import argparse

import torch

from cardinality_configs import default_configs, feature_selection_ablation_configs
from cardinality_test import SubqueryMode, job_light_only_test, job_only_test, stats_only_test, job_light_test, imdb_ceb_test, get_competitors_job_light, \
    get_competitors_job, get_competitors_stats
from data.query_db import QueryDB
from dsb_test import dsb_baselines, dsb_test


def main():
    parser = argparse.ArgumentParser(description='JGMP Experiment')
    # port
    parser.add_argument('--port', type=int, default='5432')
    # query set
    parser.add_argument('--query_set', type=str, required=True, choices=['job-light', 'job', 'imdb-ceb', 'stats-ceb', 'dsb'])
    # experiment type
    parser.add_argument('--type', type=str, required=True, choices=['cross-validation', 'incremental', 'baseline'])
    # grouped
    parser.add_argument('--grouped', action='store_true')
    # subplan query mode
    parser.add_argument('--subplan_query_mode', type=str, default='pg_selected', choices=['all', 'pg_selected'])
    # competitors
    parser.add_argument('--competitors', type=str, default='default', choices=['default', 'fs-ablation'])

    args = parser.parse_args()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    query_db = QueryDB("query-optimization", port=args.port)

    if args.subplan_query_mode == 'all':
        subplan_query_mode = SubqueryMode.ALL
    elif args.subplan_query_mode == 'pg_selected':
        subplan_query_mode = SubqueryMode.BEST
    else:
        raise NotImplementedError()

    if args.competitors == 'default':
        configs = default_configs()
    elif args.competitors == 'fs-ablation':
        configs = feature_selection_ablation_configs()
    else:
        raise NotImplementedError()

    if args.type == 'cross-validation':
        repetitions = 10
        num_parts = 5
        if args.query_set == 'job-light':
            job_light_only_test(query_db, device, subplan_query_mode, configs, repetitions=repetitions, num_parts=num_parts, port=args.port)
        elif args.query_set == 'job':
            job_only_test(query_db, device, subplan_query_mode, configs, repetitions=repetitions, num_parts=num_parts, grouped=args.grouped, port=args.port)
        elif args.query_set == 'stats-ceb':
            stats_only_test(query_db, device, subplan_query_mode, configs, repetitions=repetitions, num_parts=num_parts, port=args.port)
        elif args.query_set == 'dsb':
            dsb_test("/home/jgmp/data/dsb", configs, subplan_query_mode, 10, repetitions=repetitions, num_parts=num_parts, port=args.port)
        else:
            raise NotImplementedError()
    elif args.type == 'incremental':
        repetitions = 10
        if args.query_set == 'job-light':
            job_light_test(query_db, device, configs, repetitions=repetitions, port=args.port)
        elif args.query_set == 'imdb-ceb':
            imdb_ceb_test(query_db, device, configs, subplan_query_mode, repetitions=repetitions, port=args.port)
        else:
            raise NotImplementedError()
    elif args.type == 'baseline':
        if args.query_set == 'job-light':
            get_competitors_job_light(query_db, port=args.port)
        elif args.query_set == 'job':
            get_competitors_job(query_db, port=args.port)
        elif args.query_set == 'stats-ceb':
            get_competitors_stats(query_db, port=args.port)
        elif args.query_set == 'dsb':
            dsb_baselines("/home/jgmp/data/dsb", 10, port=args.port)
        else:
            raise NotImplementedError()
    else:
        raise NotImplementedError()

if __name__ == "__main__":
    main()

