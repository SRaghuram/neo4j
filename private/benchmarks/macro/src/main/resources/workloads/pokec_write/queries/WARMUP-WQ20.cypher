MATCH (a:PROFILES { _key: { key }})
MATCH (a)-[:KNOWS]->(b:PROFILES)-[:KNOWS]->(a)
RETURN a._key, b._key