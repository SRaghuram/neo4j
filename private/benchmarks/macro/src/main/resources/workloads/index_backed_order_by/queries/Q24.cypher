MATCH (n:PROFILES)
WHERE n.pets < "g" OR n.pets > "k"
RETURN n.pets
