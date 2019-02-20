MATCH (n:OSMNode)
WHERE exists(n.osm_id)
RETURN count(n)