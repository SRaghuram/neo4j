MATCH (n:Album { releasedIn: { year }})
WITH n
LIMIT 10
MATCH (n)<-[:APPEARS_ON]-(m:Track)
DETACH DELETE m, n