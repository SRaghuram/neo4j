MATCH (n:OSMNode)
WHERE exists(n.created)
RETURN count(n)