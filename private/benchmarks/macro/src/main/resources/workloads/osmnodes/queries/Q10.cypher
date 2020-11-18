MATCH (n:OSMNode)
WHERE n.location IS NOT NULL
RETURN n.location ORDER BY n.location