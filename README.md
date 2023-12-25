
# JGMP

### Setup

Simply build the docker image and run it:
```bash
$ sudo docker build -t jgmp_img .
$ sudo docker run --name jgmp -d jgmp_img
```
Everything (test databases, training data, dependencies, scripts, etc.) is included in the image.


### Usage

The experiments can be run with the jgmp_experiments.py script. For example:

* Collect baseline runtimes:
    ```bash
    $ sudo docker exec -t jgmp python3 /home/jgmp/jgmp_experiment.py --query_set job-light --type baseline
    ```
* Run a cross-validation experiment:
    ```bash
    $ sudo docker exec -t jgmp python3 /home/jgmp/jgmp_experiment.py --query_set job --type cross-validation --subplan_query_mode pg_selected --grouped
    ```
* Run an experiment with varying training data:
    ```bash
    $ sudo docker exec -t jgmp python3 /home/jgmp/jgmp_experiment.py --query_set imdb-ceb --type incremental --subplan_query_mode pg_selected
    ```

The results will be stored in the /home/jgmp/results/cardinality_estimation directory. The result folder for an experiment contains training logs, model weights, used samples, cardinality estimates for each subplan query, and runtimes for each query. Cardinality estimates are stored in csv files with three columns (query id, model number, and cardinality estimate). Runtimes are stored in csv files with four columns (query id, model number, inference time, and execution time).

Query ids refer to the id column in the table queries in the query-optimization database. The queries can be loaded using the class in the data/query_db.py file.


### Cardinality Injection Patch for PostgreSQL

Our patch to inject cardinality estimates into PostgreSQL is included in the end-to-end-cardest folder. It is an extension of earlier work by Han et al. (https://github.com/Nathaniel-Han/End-to-End-CardEst-Benchmark).
We have extended the patch to support a wider range of queries including non-inner joins and complex predicates.

To extract the order in which PostgreSQL estimates the cardinality of subplan queries enable the following parameters and run the query:
```SQL
   SET print_sub_queries = TRUE;
   SET print_single_tbl_queries = TRUE;
 ```
With this command we generate files containing the order of join subplan queries (multi_table_estimations.txt) and single-table subplan queries (single_table_subplan_order.txt) in PostgreSQL's data directory. The subplan queries are given in a JSON format. For example, a single-table subplan query is given as:
```JSON
{"id": 5,
"table": "date_dim",
"table_alias": "date_dim",
"rows": 361,
"width": 0,
"predicates": ["date_dim.d_year = 2001"]}
```
or a join subplan query as:
```JSON
{"id": 6,
"join_type": "Inner",
"join_clauses": ["store_sales.ss_store_sk = store.s_store_sk"],
"table_aliases": ["store_sales", "store"],
"rows": 102677,
"width": 44,
"children": [{"id": 0, "rows": 102677}, {"id": 1, "rows": 12}]}
```

Each subplan query is given an id. Join subplan queries are recursively defined using the ids of previously enumerated subplan queries.
This format also simplifies the parsing of complex queries. For an example of the parsing see the function extract_parse_subplans in the file cardinality_injection.py.

To inject the cardinality estimates into PostgreSQL set the following parameters:
```SQL
SET ml_cardest_enabled = TRUE;
SET ml_joinest_enabled = TRUE;
SET query_no = 0;
SET join_est_no = 0;
SET ml_cardest_fname = 'single_table_estimations.txt';
SET ml_joinest_fname = 'multi_table_estimations.txt';
```
Create the two files containing the estimates in the previously extracted order. Each line should contain one estimate.
For subplan queries not supported by an estimator -1 can be injected.
In this case, PostgreSQL uses its own estimator based on the estimates of the subplan queries children.
After this setup, the query can be executed as usual and PostgreSQL will read the cardinality estimates from the files.
