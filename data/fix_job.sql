INSERT INTO query_cardinalities(query_id, cardinality_estimator_id, estimate)
SELECT q.id, 1, sqc.estimate
FROM queries q
JOIN queries sq ON sq.subquery_of_id = q.id
JOIN node_queries nsq ON nsq.query_id = sq.id
JOIN query_cardinalities AS sqc ON sqc.query_id = sq.id
WHERE sqc.cardinality_estimator_id = 1
GROUP BY q.id, sq.id, sqc.estimate
HAVING (q.id, COUNT(nsq.node_id)) IN (SELECT q2.id, MAX(node_count) AS max_nodes
                            				 FROM (SELECT sq2.subquery_of_id AS qid, COUNT(nsq2.node_id) AS node_count
                                           FROM node_queries nsq2
                                           JOIN queries sq2 ON sq2.id = nsq2.query_id
                                           WHERE sq2.set_id = 1 AND sq2.subquery_of_id IS NOT NULL
                                           GROUP BY nsq2.query_id, sq2.subquery_of_id) sc
                                     JOIN queries q2 ON q2.id = sc.qid
                                     GROUP BY q2.id)
ORDER BY q.id;