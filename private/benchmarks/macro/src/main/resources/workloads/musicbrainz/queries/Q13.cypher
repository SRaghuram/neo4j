MATCH (a:Artist { name: { name }})-[:CREDITED_AS]->(b)-[:CREDITED_ON]->(t:Track)-[:APPEARS_ON]->(m:Medium)<-[:RELEASED_ON_MEDIUM]-(r:Release)
RETURN t.name, r.name