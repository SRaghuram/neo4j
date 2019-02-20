MATCH (s:PROFILES { _key: { key }})-[*1..2]->(n:PROFILES)
RETURN DISTINCT n._key