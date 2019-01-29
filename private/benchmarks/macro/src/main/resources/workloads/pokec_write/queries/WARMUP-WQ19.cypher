MATCH (a:PROFILES { _key: { key }})
MATCH (a)-[:KNOWS]->(b:PROFILES)-[:KNOWS]->(c:PROFILES)
RETURN a._key, b._key, c._key