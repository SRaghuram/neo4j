MATCH (n:PROFILES)
WHERE exists( (n)-[:RELATION]->() )
RETURN count(*)