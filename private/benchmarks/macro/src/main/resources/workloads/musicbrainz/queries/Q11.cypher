MATCH (a:Artist { name: { name }})-[:CREDITED_AS]->(b)-[:CREDITED_ON]->(t:Track)
RETURN t.name