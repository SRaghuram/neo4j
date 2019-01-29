MATCH (n:OSMNode)
WHERE n.name = $name
RETURN n.name