MATCH (i:Intermediary)
WHERE size( (i)--() ) > 100
MATCH (i)-[connection]-(entity)
RETURN i.name as intermediary, type(connection) as relationship, head(labels(entity)) as type, count(*) as count
ORDER BY count DESC
LIMIT 20