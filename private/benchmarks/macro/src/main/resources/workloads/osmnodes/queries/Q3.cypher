MATCH (n:OSMNode)
WHERE exists(n.location)
RETURN count(n)