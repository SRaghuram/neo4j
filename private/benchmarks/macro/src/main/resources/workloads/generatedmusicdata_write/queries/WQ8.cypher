MATCH (a:Artist)-[:WORKED_WITH*..5 { year: { year }}]->(b:Artist)
MERGE (a)-[r:COLLABORATOR { year: { year }}]->(b)
SET r.updated_at = timestamp()