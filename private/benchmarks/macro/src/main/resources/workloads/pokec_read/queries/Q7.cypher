MATCH (s:PROFILES { _key: { from }}),(t:PROFILES { _key: { to }}), p = shortestPath((s)-[:RELATION*..15]->(t))
RETURN [x IN nodes(p)| x._key] AS path
