MATCH (n:PROFILES)
WHERE n.pets = 'pes' OR n.children = 'nemam'
RETURN n