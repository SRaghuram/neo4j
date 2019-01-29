MATCH (s:PROFILES { _key: { key }})-[*5..5]->(n:PROFILES)
RETURN DISTINCT n._key