MATCH (a:PROFILES)
WHERE a.pets IS NOT NULL AND a.children IS NOT NULL
RETURN a.pets, a.children ORDER BY a.pets, a.children LIMIT 10000
