ALTER TABLE query_cardinalities
ADD COLUMN estimation_latency float
DEFAULT NULL;


SELECT rq.id, rq.name, COUNT(Distinct qn.node_id) AS node_count, COUNT(DISTINCT q.id) AS subquery_count, COUNT(qn.node_id) AS total_subquery_node_count
FROM queries rq
JOIN query_sets AS s ON s.id = rq.set_id
JOIN queries q ON q.subquery_of_id = rq.id
JOIN node_queries qn ON qn.query_id = q.id
WHERE s.name = 'job'
GROUP BY rq.id;