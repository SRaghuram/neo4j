MATCH (p1:Person {id:$Person})-[:KNOWS]->(p2:Person)
CALL {
  WITH p2
  MATCH (p2)-[:KNOWS]->()-[:KNOWS]->(p3:Person)
  WHERE NOT (p2)-[:KNOWS]->(p3)
  RETURN count(*) AS c
}
RETURN p1.id, p2.id, c