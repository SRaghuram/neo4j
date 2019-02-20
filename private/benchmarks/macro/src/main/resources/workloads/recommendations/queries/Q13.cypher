MATCH (p1:Person { first_name: { first_name }})-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person),(p1)-[:WORKED_FOR|:WORKS_FOR]->(co:Company)<-[:WORKED_FOR]-(p2)
WHERE NOT ((p1)-[:WORKS_FOR]->(co)<-[:WORKS_FOR]-(p2))
WITH p1,p2,c,co
MATCH (p1)-[:FRIEND_OF]-()-[:FRIEND_OF]-(p2)
RETURN p1.first_name+' '+p1.last_name as Person1, p2.first_name+' '+p2.last_name AS Person2, collect(DISTINCT c.name), collect(DISTINCT co.name) AS Company