MATCH (n:Track)
WITH n
LIMIT 1000
SET n:MyTrack
REMOVE n:Track