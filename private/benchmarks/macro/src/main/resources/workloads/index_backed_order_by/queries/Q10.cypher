MATCH (n:PROFILES)
WHERE n.pets < "i"
RETURN n.pets ORDER BY n.pets DESC
