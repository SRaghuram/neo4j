MATCH (n:Album { releasedIn: { year }})
WHERE NOT exists(n.tracks)
WITH n
LIMIT 10
MATCH (n)<-[:APPEARS_ON]-(m:Track)
WITH n, collect(m) AS tracks
WITH n, tracks, extract(t IN tracks | t.title) AS titles
SET n.tracks = titles
WITH tracks
UNWIND (tracks) AS track
DETACH DELETE track