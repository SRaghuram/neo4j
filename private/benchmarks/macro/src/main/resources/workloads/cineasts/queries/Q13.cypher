MATCH (l:Actor { name: { name }})-[:ACTS_IN]->(m:Movie)<-[:ACTS_IN]-(c:Actor)
RETURN count(*), c.name
ORDER BY count(*) DESC , c.name
LIMIT 20