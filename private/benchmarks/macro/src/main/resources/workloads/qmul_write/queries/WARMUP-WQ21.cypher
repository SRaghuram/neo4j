MATCH (p:Person { name: { name }})-[:friends]->(f:Person)
RETURN p, f