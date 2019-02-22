MATCH (n:PROFILES)
WHERE n.children STARTS WITH ""
RETURN n.children, n.pets ORDER BY n.children, n.pets
