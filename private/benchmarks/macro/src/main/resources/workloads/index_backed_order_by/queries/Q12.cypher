MATCH (n:PROFILES)
WHERE n.pets < "i" AND n.cars IS NOT NULL
RETURN n.pets, n.cars ORDER BY n.pets, n.cars
