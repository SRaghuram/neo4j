MATCH (a:PROFILES { _key: { key }})
MERGE (a)-[:KNOWS]->(b:PROFILES)
ON CREATE SET b._key = 2000000+a._key