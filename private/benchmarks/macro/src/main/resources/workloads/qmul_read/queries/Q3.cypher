MATCH (a:Person)-->(m)-[r]->(n)-->(ax)
  WHERE ax = a AND a.uid IN [{uid1}, {uid2}]
WITH m, n, r
  WHERE exists(m.location_lat) AND exists(n.location_lat)
RETURN count(r)
