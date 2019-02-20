MATCH (t:Track)
MERGE (al:Album)<-[:APPEARS_ON]-(t)
MERGE (a:Artist)-[:CREATED]->(al)
RETURN *