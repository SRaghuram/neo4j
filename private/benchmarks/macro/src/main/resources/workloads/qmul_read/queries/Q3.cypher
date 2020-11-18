MATCH (a:Person)-->(m)-[r]->(n)-->(ax)
  WHERE ax = a AND a.uid IN [$uid1, $uid2]
WITH m, n, r
  WHERE m.location_lat IS NOT NULL AND n.location_lat IS NOT NULL
RETURN count(r)
