MATCH (s:PROFILES { _key: { key }})-[:RELATION*2..2]->(n:PROFILES)
RETURN DISTINCT n._key