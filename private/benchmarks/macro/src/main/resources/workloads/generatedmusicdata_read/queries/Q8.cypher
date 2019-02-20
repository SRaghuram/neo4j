MATCH (a)-[r]->(b)
  WHERE id(r) = {id}
RETURN *
