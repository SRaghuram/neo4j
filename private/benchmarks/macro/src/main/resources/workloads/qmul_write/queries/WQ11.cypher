MATCH (p1:Person)
CREATE (p2:Person { name: p1.name + ' 2' })
RETURN p2.name