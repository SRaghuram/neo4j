MATCH (p:Person)-[b:BOUGHT]->(prod1:Product)-[:MADE_BY]->(br:Brand { name: { name }})
RETURN p.first_name+' '+p.last_name AS Person, collect(prod1.name) AS OwnedProducts
ORDER BY Person ASC