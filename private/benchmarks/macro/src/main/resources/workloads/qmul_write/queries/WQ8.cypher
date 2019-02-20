MATCH (p1:Person { name: { name1 }}),(p2:Person { name: { name2 }})
CREATE (p3:Person { name: p1.name + ' & ' + p2.name })
RETURN p3.name