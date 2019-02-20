MATCH (a:PROFILES { _key: { key }})
MATCH (a)-[:KNOWS]->(b:PROFILES)
RETURN a._key, b._key