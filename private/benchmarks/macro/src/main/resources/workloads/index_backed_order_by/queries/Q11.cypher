MATCH (n:PROFILES)
WHERE exists(n.pets)
RETURN n.pets ORDER BY n.pets ASC LIMIT 10000
