CREATE TABLE query_languages(
    id integer PRIMARY KEY,
    name text NOT NULL UNIQUE
);

INSERT INTO query_languages(id, name) VALUES
(1, 'sql'),
(2, 'cypher');

CREATE TABLE databases(
    id integer PRIMARY KEY,
    name text NOT NULL UNIQUE,
    language_id integer REFERENCES query_languages(id) ON DELETE CASCADE NOT NULL
);

INSERT INTO databases(id, name, language_id) VALUES
(1, 'imdb', 1),
(2, 'snb', 2);

CREATE TABLE query_sets(
    id SERIAL PRIMARY KEY,
    name text NOT NULL,
    database_id integer REFERENCES databases(id) ON DELETE CASCADE NOT NULL
);

CREATE TABLE queries(
    id SERIAL PRIMARY KEY,
    set_id integer REFERENCES query_sets(id) ON DELETE CASCADE NOT NULL,
    subquery_of_id integer REFERENCES queries(id) ON DELETE CASCADE,
    name text,
    UNIQUE(set_id, name)
);

CREATE TABLE labels(
    id SERIAL PRIMARY KEY,
    database_id integer REFERENCES databases(id) ON DELETE CASCADE NOT NULL,
    is_node_label boolean NOT NULL,
    name text NOT NULL,
    UNIQUE(database_id, is_node_label, name)
);

CREATE TABLE elements(
    id SERIAL PRIMARY KEY
);

CREATE TABLE element_labels(
    element_id integer REFERENCES elements(id) ON DELETE CASCADE,
    label_id integer REFERENCES labels(id) ON DELETE CASCADE,
    PRIMARY KEY(element_id, label_id)
);

CREATE TABLE nodes(
    id integer REFERENCES elements(id) PRIMARY KEY,
    virtual bool NOT NULL
);

CREATE TABLE node_queries(
    node_id integer REFERENCES nodes(id) ON DELETE CASCADE,
    query_id integer REFERENCES queries(id) ON DELETE CASCADE,
    PRIMARY KEY(node_id, query_id)
);

CREATE TABLE cardinality_estimators(
    id integer PRIMARY KEY,
    name text NOT NULL,
    time timestamp,
    info text
);

INSERT INTO cardinality_estimators(id, name) VALUES
(1, 'true'),
(2, 'postgres'),
(3, 'neo4j');

CREATE TABLE data_roles(
    id integer PRIMARY KEY,
    name text NOT NULL UNIQUE
);

INSERT INTO data_roles(id, name) VALUES
(1, 'training'),
(2, 'validation'),
(3, 'test');

CREATE TABLE node_cardinalities(
    node_id integer REFERENCES nodes(id) ON DELETE CASCADE,
    cardinality_estimator_id integer REFERENCES cardinality_estimators(id),
    estimate float NOT NULL,
    PRIMARY KEY(node_id, cardinality_estimator_id)
);

CREATE TABLE query_cardinalities(
    query_id integer REFERENCES queries(id) ON DELETE CASCADE,
    cardinality_estimator_id integer REFERENCES cardinality_estimators(id),
    estimate float NOT NULL,
    data_role_id integer REFERENCES data_roles(id),
    estimation_latency float,
    PRIMARY KEY(query_id, cardinality_estimator_id)
);

CREATE TABLE directions(
    id integer PRIMARY KEY,
    name text NOT NULL UNIQUE
);

INSERT INTO directions(id, name) VALUES
(1, 'natural'),
(2, 'reversed'),
(3, 'undirected');

CREATE TABLE edges(
    id integer REFERENCES elements(id) ON DELETE CASCADE PRIMARY KEY,
    direction_id integer REFERENCES directions(id) NOT NULL,
    from_id integer REFERENCES nodes(id) ON DELETE CASCADE NOT NULL,
    to_id integer REFERENCES nodes(id) ON DELETE CASCADE NOT NULL
);

CREATE TABLE comparison_operators(
    id integer PRIMARY KEY,
    name text NOT NULL
);

INSERT INTO comparison_operators(id, name) VALUES
(1, '='),
(2, '<'),
(3, '>'),
(4, 'LIKE'),
(5, 'IS');

CREATE TABLE disjunctions(
    id SERIAL PRIMARY KEY,
    element_id integer REFERENCES elements(id) ON DELETE CASCADE NOT NULL
);

CREATE TABLE predicates(
    id SERIAL PRIMARY KEY,
    disjunction_id integer REFERENCES disjunctions(id) ON DELETE CASCADE NOT NULL,
    attribute_name text NOT NULL,
    operator_id integer REFERENCES comparison_operators(id) NOT NULL,
    value text,
    positive boolean NOT NULL
);