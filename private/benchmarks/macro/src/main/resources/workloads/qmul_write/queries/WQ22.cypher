MATCH (p:Person { name: { name }})-[r:friends]->(f:Person)
DELETE r
MERGE (p)-[:friends]->(f)