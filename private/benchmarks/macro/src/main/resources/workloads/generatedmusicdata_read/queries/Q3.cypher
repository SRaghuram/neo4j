MATCH (a:Artist)-[:CREATED]->(al:Album)
  WHERE al.releasedIn = 1979
RETURN *
