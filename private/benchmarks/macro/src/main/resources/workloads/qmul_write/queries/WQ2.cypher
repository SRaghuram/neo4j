MATCH (p1:Person { name: { name1 }})
CREATE (p2:Person { name: { name2 }+ ' 2' })
RETURN p2.name