MATCH (a { _key: { _key }})-[:RELATION]->(b)
OPTIONAL MATCH (b)-[:RELATION]->(a)
RETURN count(*)