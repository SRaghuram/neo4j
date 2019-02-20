MATCH (p1:PROFILES { _key: { key }})
MERGE (n:Copy { key: p1._key })
ON CREATE SET n = p1