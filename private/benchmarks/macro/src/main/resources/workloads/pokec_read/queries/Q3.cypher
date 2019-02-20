MATCH (:PROFILES { _key: { key }})-[:RELATION]->(n:PROFILES)
RETURN n._key