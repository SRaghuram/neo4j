MATCH (n:PROFILES)
WHERE n.children STARTS WITH ""
RETURN n.children, n.pets ORDER BY n.children, n.pets
SKIP 475697 / 2 // Skip half of the results
