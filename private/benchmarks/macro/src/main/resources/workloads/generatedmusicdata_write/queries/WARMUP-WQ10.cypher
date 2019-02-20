MATCH (n:Album { releasedIn: { year }})
WITH n
LIMIT 10
MATCH (n)--(m)
RETURN n.releasedIn, count(m)