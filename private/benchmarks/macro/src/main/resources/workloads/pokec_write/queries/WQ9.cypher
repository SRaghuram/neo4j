MATCH (n1:PROFILES { _key: { from }}),(n2:PROFILES { _key: { to }})
MERGE (n1)-[:BLIND_DATE]->(n2)
