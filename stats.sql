SELECT t1.QueryId, 
  ROUND(AVG(t1.Seconds)::numeric,6) AS Algebra,
  ROUND(AVG(t2.Seconds)::numeric, 6) AS OLAP
FROM talgebra_queries t1 FULL OUTER JOIN tolap_queries t2
  ON t1.QueryId = t2.QueryId 
GROUP BY t1.QueryId
UNION
SELECT t2.QueryId, NULL AS Algebra,
  ROUND(AVG(t2.Seconds)::numeric, 6) AS OLAP
FROM tolap_queries t2
WHERE t2.QueryId IS NOT NULL AND NOT EXISTS (
  SELECT * FROM talgebra_queries t1 
  WHERE t1.QueryId = t2.QueryId )
GROUP BY t2.QueryId
ORDER BY 1;

