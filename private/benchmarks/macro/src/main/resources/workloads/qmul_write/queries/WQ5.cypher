CREATE (p1:Person { name: $name1 + ' 2' })
CREATE (p2:Person { name: $name2 + ' 2' })
CREATE (p1)-[:KNOWS]->(p2)
RETURN p1.name