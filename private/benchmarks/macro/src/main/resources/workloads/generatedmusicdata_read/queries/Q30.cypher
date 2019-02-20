MATCH (a:Artist)-[:WORKED_WITH*..5 {year: {year}}]->(b:Artist)
RETURN *
