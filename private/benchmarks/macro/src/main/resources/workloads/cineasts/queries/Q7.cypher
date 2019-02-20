MATCH (u:User { login: { login }})-[:FRIEND]-()-[r:RATED]->(m:Movie)
RETURN m.title, avg(r.stars), count(*)
ORDER BY AVG(r.stars) DESC , count(*) DESC