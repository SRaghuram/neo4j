WITH datetime($date) AS d
MATCH (n:OSMNode)
WHERE n.created = d
RETURN n.name, toString(n.created)