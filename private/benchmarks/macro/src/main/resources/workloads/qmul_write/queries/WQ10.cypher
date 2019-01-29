MATCH (p1:Person)
WITH p1
LIMIT 1000
CREATE (p2:Person { name: p1.name + ' 2' })
RETURN p2.name