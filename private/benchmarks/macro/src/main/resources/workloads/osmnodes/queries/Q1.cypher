MATCH (n:OSMNode)
WHERE n.osm_id IS NOT NULL
RETURN count(n)