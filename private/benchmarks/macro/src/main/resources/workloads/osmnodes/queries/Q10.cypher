MATCH (n:OSMNode)
WHERE exists(n.location)
RETURN n.location ORDER BY n.location