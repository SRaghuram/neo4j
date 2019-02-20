MATCH (p1:Person)-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person),(p1)-[:WORKED_FOR|:WORKS_FOR]->(co:Company)<-[:WORKED_FOR]-(p2)
RETURN co.name AS Company, c.name AS Competency, count(p2) AS Count
ORDER BY Count DESC LIMIT 10