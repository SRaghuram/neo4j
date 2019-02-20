MATCH (a:Person)-->(m)-[r]->(n)-->(a)
  WHERE a.uid IN [{uid1}, {uid2}] AND exists(m.location_lat) AND exists(n.location_lat)
RETURN count(r)
