MATCH (n:PROFILES)
WHERE n.gender >= 0
RETURN n.gender, n.pets ORDER BY n.gender, n.pets LIMIT 400000
