MATCH (a:PROFILES)
WHERE exists(a.pets) AND exists(a.children)
RETURN a.pets, a.children ORDER BY a.pets, a.children LIMIT 10000
