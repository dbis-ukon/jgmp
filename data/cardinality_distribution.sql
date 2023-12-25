SELECT qs.name,
			 AVG(LOG(GREATEST(qc.estimate, 1))),
       VARIANCE(LOG(GREATEST(qc.estimate, 1))),
       LOG(GREATEST(PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY qc.estimate), 1)) AS fiveper,
       LOG(GREATEST(PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY qc.estimate), 1)) AS twentyper,
       LOG(GREATEST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qc.estimate), 1)) AS median,
       LOG(GREATEST(PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY qc.estimate), 1)) AS eightyper,
       LOG(GREATEST(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY qc.estimate), 1)) AS ninetyfiveper
FROM query_sets qs
JOIN queries q ON q.set_id = qs.id
JOIN query_cardinalities qc ON qc.query_id = q.id
JOIN cardinality_estimators ce ON ce.id = qc.cardinality_estimator_id
JOIN databases d ON d.id = qs.database_id
JOIN query_languages ql ON ql.id = d.language_id
WHERE ce.name = 'true' AND ql.name = 'sql'
GROUP BY qs.id;