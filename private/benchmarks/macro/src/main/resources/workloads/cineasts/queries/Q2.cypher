MATCH (a:Actor)-[:ACTS_IN]->(m:Movie)
WITH a, count(m) AS movie_count
WHERE movie_count >= 20
RETURN a, movie_count
ORDER BY movie_count DESC