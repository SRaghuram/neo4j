CREATE (p2:Person { name: { name2 }+ ' 2' })
WITH p2
MATCH (p1:Person { name: { name1 }})
CREATE (p1)-[:KNOWS]->(p2)
RETURN p1.name