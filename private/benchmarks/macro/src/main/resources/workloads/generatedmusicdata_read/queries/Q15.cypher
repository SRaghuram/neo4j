MATCH (a:Artist)
OPTIONAL MATCH (b:Artist)-[:WORKED_WITH]->(a)
RETURN *
