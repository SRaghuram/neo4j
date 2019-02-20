MATCH (n:OSMNode)
WHERE datetime('2015-01-01')< n.created < datetime('2015-01-09')
RETURN count(n)