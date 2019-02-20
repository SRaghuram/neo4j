MATCH (al:Album)
  WHERE (:Artist)-[:CREATED]->(al) AND (al)<-[:APPEARS_ON]-(:Track)
RETURN *
