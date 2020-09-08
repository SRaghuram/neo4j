MATCH path=(p1:Person {id:$Person})-[:KNOWS]->()-[:KNOWS]->(p2:Person)
WHERE NOT (p1)-[:KNOWS]->(p2)
RETURN path