MATCH (n:Album { releasedIn: { year }})
WITH n
LIMIT 10
MERGE (y:ReleaseYear { year: { year }})
MERGE (n)-[:RELEASED_IN]->(y)
REMOVE n.releasedIn