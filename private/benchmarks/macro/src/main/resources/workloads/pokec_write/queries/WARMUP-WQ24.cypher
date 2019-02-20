MATCH (n:PROFILES)
WHERE exists(n.AGE)
WITH n
LIMIT 50000
WITH n.AGE AS age, collect(n) AS nodes
RETURN count(*)