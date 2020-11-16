MATCH (n:PROFILES)
WHERE n.gender >= 0
RETURN n.gender, n.pets ORDER BY n.gender, n.pets
SKIP 1000000 // Skip the whole first chunk
