MATCH (a:PROFILES { _key: $key }),(b:PROFILES)
WHERE a.AGE IS NOT NULL AND b.AGE IS NOT NULL AND abs(a.AGE - b.AGE)< 5 AND b.pets = 'pes' AND b.children = 'mam'
RETURN b