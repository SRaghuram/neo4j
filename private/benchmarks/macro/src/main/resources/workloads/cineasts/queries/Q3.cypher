MATCH (a:Actor)-[:ACTS_IN]->(m:Movie)
WITH a, collect(m.title) AS movies
WHERE size(movies)>= 20
RETURN a, movies
ORDER BY size(movies) DESC