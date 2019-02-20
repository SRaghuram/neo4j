MATCH (n:OSMNode)
WHERE exists(n.name)
RETURN count(n)