MATCH (n:PROFILES)
WHERE n.pets < "m"
RETURN n.pets
