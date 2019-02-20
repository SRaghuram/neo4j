MATCH (p1:Person)-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person),
      (p1)-->(co:Company)<--(p2)
RETURN p1.first_name+' '+p1.last_name as Person1, p2.first_name+' '+p2.last_name AS Person2, collect(DISTINCT c.name), collect(DISTINCT co.name) AS Company
LIMIT 50
