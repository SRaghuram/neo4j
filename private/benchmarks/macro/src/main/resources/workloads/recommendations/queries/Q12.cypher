MATCH p=allShortestPaths((source:Person)-[:FRIEND_OF*]-(target:Person))
WHERE id(source)< id(target)
WITH p
WHERE length(p)> 1
UNWIND nodes(p)[1..-1] AS n
RETURN n.first_name, n.last_name, count(*) AS betweenness
ORDER BY betweenness DESC