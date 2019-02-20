WITH point({ latitude: $latitude, longitude: $longitude }) AS p
MATCH (n:OSMNode)
WHERE n.location = p
RETURN n.name, n.location.x, n.location.y