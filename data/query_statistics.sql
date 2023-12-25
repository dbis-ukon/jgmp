

-- # tables
SELECT COUNT(DISTINCT el.label_id)
FROM queries q
JOIN node_queries nq ON nq.query_id = q.id
JOIN element_labels el ON el.element_id = nq.node_id
WHERE set_id = 1 AND subquery_of_id IS NULL;

-- # join types
SELECT COUNT(DISTINCT el.label_id)
FROM queries q
JOIN node_queries nq ON nq.query_id = q.id
JOIN edges e ON e.from_id = nq.node_id
JOIN element_labels el ON el.element_id = e.id
WHERE set_id = 1 AND subquery_of_id IS NULL;

-- # attributes
SELECT COUNT(DISTINCT (el.label_id, p.attribute_name))
FROM queries q
JOIN node_queries nq ON nq.query_id = q.id
JOIN element_labels el ON el.element_id = nq.node_id
JOIN disjunctions d ON d.element_id = nq.node_id
JOIN predicates p ON p.disjunction_id = d.id
WHERE set_id = 1 AND subquery_of_id IS NULL;


-- # queries
SELECT COUNT(*)
FROM queries
WHERE set_id = 1 AND subquery_of_id IS NULL;


-- # joins per query
SELECT MIN(joincount), MAX(joincount)
FROM
(SELECT COUNT(nq.node_id) - 1 AS joincount
FROM queries q
JOIN node_queries nq ON nq.query_id = q.id
WHERE set_id = 1 AND subquery_of_id IS NULL
GROUP BY q.id) as jcquery;


-- # multiple instances of same table
SELECT MAX(labelcount)
FROM
(SELECT COUNT(*) AS labelcount
FROM queries q
JOIN node_queries nq ON nq.query_id = q.id
JOIN element_labels el ON el.element_id = nq.node_id
WHERE set_id = 1 AND subquery_of_id IS NULL
GROUP BY q.id, el.label_id) AS labelquery;

-- # subplan queries per query
SELECT q.id, COUNT(*) AS cnt
FROM queries q
JOIN queries sq ON sq.subquery_of_id = q.id
WHERE q.set_id = 1
GROUP BY q.id
ORDER BY cnt;
