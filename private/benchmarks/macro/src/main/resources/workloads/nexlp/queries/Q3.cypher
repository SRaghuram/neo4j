MATCH (writer:Entity { Id: { eId }}),(writer)-[:wrote]->(segment)-[r1:regarding_entity]->(entity)
WITH entity AS names, count(entity) AS counts
ORDER BY count(entity) DESC LIMIT 100
WITH names, counts
MATCH (otherwriters)-[:wrote]->(segment)-[r2:regarding_entity]->(names)
RETURN otherwriters.Name AS Name, otherwriters.Id AS Id, count(DISTINCT names) AS EntityCount
ORDER BY count(DISTINCT names) DESC SKIP 1
LIMIT 3;