MATCH (s:PROFILES { _key: { key }})-[*4..4]->(n:PROFILES)
RETURN DISTINCT n._key