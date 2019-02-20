MATCH (n:OSMNode)
WHERE 'Da' < n.name < 'Dre'
RETURN count(n)