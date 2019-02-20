MATCH (p1:Person { name: { name1 }})
CREATE (p2:PersonX { name: { name2 }})
RETURN p2.name