MATCH (t:Track)-[:APPEARS_ON]->(a:Album)
  WHERE t.duration IN [60, 61, 62, 63, 64]
RETURN *
