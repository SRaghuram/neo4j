MATCH (p1:PROFILES { _key: { key }})
MERGE (p1)-[:COPY]->(n:Copy { key: toInt(p1._key / 2)})
ON CREATE SET n += p1
ON MATCH SET p1:HAS_COPY