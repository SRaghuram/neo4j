MATCH (a:PROFILES { _key: { key }})-[r:RELATION]->(b)
RETURN a._key, b._key, r