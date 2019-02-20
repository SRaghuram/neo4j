MATCH (a:Artist)-[:WORKED_WITH]->(b:Artist)-[:WORKED_WITH]->(c:Artist)-[:WORKED_WITH]->(a)
RETURN *
