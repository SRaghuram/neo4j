MATCH (a1:Artist)-[:WORKED_WITH *4]->(a2:Artist)
  WHERE id(a1) = {id}
RETURN DISTINCT a2
