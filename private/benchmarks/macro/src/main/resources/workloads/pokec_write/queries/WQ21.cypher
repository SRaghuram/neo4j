MATCH (a:PROFILES { _key: { key }})
MATCH (b:PROFILES { _key: { key }+1 })
MERGE (a)-[:KNOWS]->(c:PROFILES)-[:KNOWS]->(b)
ON CREATE SET c._key = 2000000+a._key