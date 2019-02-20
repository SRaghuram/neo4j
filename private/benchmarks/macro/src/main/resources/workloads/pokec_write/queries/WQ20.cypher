MATCH (a:PROFILES { _key: { key }})
MERGE (a)-[:KNOWS]->(b:PROFILES)-[:KNOWS]->(a)
ON CREATE SET b._key = 2000000+a._key