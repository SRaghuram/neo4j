MATCH (a { _key: { _key }})-[:RELATION]->(b)
MATCH (b)-[:RELATION]->(a)
RETURN count(*)