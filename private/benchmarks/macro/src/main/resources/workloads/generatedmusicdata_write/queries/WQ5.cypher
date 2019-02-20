MATCH (a)-[r]->(b)
WHERE id(r)= { id }
CREATE (n:Track { fromid: { id }})
RETURN n.fromid