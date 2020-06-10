MATCH (a:Officer),(b:Officer)
WHERE a.name CONTAINS $officer1 AND b.name CONTAINS $officer2
MATCH p=allShortestPaths((a)-[:OFFICER_OF|:INTERMEDIARY_OF|:REGISTERED_ADDRESS*..10]-(b))
RETURN p
LIMIT 50