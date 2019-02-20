MATCH (artist:Artist)
  WHERE NOT (artist)-[:CREATED]->(:Album {releasedIn: 1975})
RETURN *
