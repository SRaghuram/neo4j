MATCH (t:Track)-[:APPEARS_ON]->(a:Album)
  WHERE id(a) = {id}
RETURN *
