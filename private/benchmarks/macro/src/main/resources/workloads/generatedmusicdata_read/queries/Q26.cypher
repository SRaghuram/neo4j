MATCH p = shortestPath((a1:Artist)-[:WORKED_WITH]->(a2:Artist))
  WHERE id(a1) = {id1} AND id(a2) = {id2}
RETURN p
