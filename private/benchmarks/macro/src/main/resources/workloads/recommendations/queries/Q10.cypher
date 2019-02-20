MATCH (p1:Person { name:'Shawna Luna' })-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person)
WITH p1,p2
MATCH (p1)-->(co:Company)<--(p2),(prod1:Product)<-[:BOUGHT]-(p1)-[:FRIEND_OF]-(p2)-[:BOUGHT]->(prod1)
WITH p1, p2, prod1
MATCH (prod1)-[:MADE_BY]->(b:Brand)<-[:MADE_BY]-(prod2:Product)
WHERE NOT ((p1)-[:BOUGHT]->(prod2))
RETURN p1.name, prod1.name, prod2.name
LIMIT 10