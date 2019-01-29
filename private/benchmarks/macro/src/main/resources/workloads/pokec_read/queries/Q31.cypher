MATCH (s:PROFILES { _key: { key }})-[*3..3]->(n:PROFILES)
RETURN DISTINCT n._key