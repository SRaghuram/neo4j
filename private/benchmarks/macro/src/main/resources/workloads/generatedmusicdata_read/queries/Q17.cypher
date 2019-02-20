MATCH (al:Album)
  WHERE (:Artist)-[:CREATED]->(al)<-[:APPEARS_ON]-(:Track)
RETURN *
