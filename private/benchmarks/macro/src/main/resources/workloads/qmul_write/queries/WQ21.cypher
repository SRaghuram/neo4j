MATCH (p:Person { name: { name }})-[:friends]->(f:Person)
DETACH DELETE f