MATCH (al:Album)
WHERE (:Artist)-[:CREATED]->(al)<-[:APPEARS_ON]-(:Track)
SET al.completed = timestamp()