MATCH (a:PROFILES { _key: { _key }})-[r:RELATION]->(b)
WITH a, COLLECT(b) AS others
SET a.knows = size(others)
FOREACH (o IN others | MERGE (o)-[:KNOWS]->(a))