MATCH (n:PROFILES)
WHERE n.children STARTS WITH ""
RETURN DISTINCT n.children, n.pets
