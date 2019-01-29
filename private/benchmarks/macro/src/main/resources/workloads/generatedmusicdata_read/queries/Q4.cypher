MATCH (a:Artist)-[:CREATED]->(al:Album)
  WHERE a.gender = 'male'
RETURN *
