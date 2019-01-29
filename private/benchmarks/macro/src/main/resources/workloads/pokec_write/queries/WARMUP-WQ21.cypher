MATCH (a:PROFILES { _key: { key }})
MATCH (b:PROFILES { _key: { key }+1 })
MATCH (a)-[:KNOWS]->(:PROFILES)-[:KNOWS]->(b)
RETURN a._key, b._key