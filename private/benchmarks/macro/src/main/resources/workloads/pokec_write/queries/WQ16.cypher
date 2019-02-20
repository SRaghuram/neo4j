MATCH (a:PROFILES { _key: { key }})-[r:RELATION]->(b)
DELETE r
CREATE (a)-[:KNOWS]->(b)