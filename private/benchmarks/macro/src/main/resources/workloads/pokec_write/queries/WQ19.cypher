MATCH (a:PROFILES { _key: { key }})
MERGE (a)-[:KNOWS]->(b:PROFILES)-[:KNOWS]->(c:PROFILES)
ON CREATE SET b._key = 2000000+a._key, c._key = 4000000+a._key