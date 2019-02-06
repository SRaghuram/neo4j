MATCH (n:PROFILES)
WHERE exists(n.pets)
RETURN n.eye_color LIMIT 50000
