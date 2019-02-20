MATCH (n:PROFILES)
WHERE n.gender = 1 OR n.hair_color = 'blond'
RETURN n