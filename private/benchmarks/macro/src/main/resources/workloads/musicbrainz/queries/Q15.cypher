MATCH (t:Track)-[:APPEARS_ON]->(m:Medium)
WHERE id(m)= { id }
RETURN *
LIMIT 50