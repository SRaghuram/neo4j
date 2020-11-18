MATCH (a:Person)-->(m)-[r]->(n)-->(a)
  WHERE a.uid IN [$uid1, $uid2] AND m.location_lat IS NOT NULL AND n.location_lat IS NOT NULL
RETURN count(r)
