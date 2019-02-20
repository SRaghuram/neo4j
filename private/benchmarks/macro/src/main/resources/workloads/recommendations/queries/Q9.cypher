MATCH (p:Person)-[b:BOUGHT]->(prod1:Product)-[:MADE_BY]->(br:Brand { name: { name }})
WITH DISTINCT p
MATCH (p)-[:HAS_COMPETENCY]->(co:Competency)
RETURN co.name, count(p)
ORDER BY count(p) DESC