MATCH (p1:Person { name: { name1 }}),(p2:Person { name: { name2 }})
CREATE (p1)-[:KNOWS]->(p2)
RETURN p1.name