MATCH (a:Artist { name: 'John Lennon' })-[:CREDITED_AS]->(b)-[:CREDITED_ON]->(t:Track)
RETURN t.name