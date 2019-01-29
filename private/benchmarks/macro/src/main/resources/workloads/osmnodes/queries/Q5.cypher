MATCH (n:OSMNode)
WHERE exists(n.place)
RETURN count(n)