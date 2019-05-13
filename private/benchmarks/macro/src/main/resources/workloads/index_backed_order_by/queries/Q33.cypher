MATCH (n:PROFILES)
WHERE n.gender >= 0
RETURN n.gender, n.pets, count(*)
