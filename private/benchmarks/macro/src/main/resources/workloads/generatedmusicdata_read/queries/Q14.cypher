MATCH (al:Album)
RETURN (:Artist)-[:CREATED]->(al)<-[:APPEARS_ON]-(:Track)
