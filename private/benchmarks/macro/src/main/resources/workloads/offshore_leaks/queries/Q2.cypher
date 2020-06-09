MATCH (intermediary:Intermediary)-[:INTERMEDIARY_OF]->(e:Entity)
WHERE intermediary.name CONTAINS $intermediary
RETURN e.jurisdiction AS country, COUNT(*) AS frequency
ORDER BY frequency DESC LIMIT 20;