MATCH (a:PROFILES { _key: { _key }})-[r:RELATION]->(b)
WITH a, COLLECT(b) AS others
RETURN a._key, size(others)