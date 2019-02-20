MATCH (a1:Artist {name: {name}})-[:PERFORMED_AT]->(:Concert)<-[:PERFORMED_AT]-(a2:Artist)
MATCH (a1)-[:SIGNED_WITH]->(corp:Company)<-[:SIGNED_WITH]-(a2)
RETURN a2, COUNT(*)
  ORDER BY COUNT(*) DESC
