MATCH (n:Project)
WHERE (n.owner='neo4j' AND n.name='neo4j')
RETURN DISTINCT n.version