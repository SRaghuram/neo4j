MATCH (a:PROFILES { _key: { key }}),(b:PROFILES)
WHERE exists(a.AGE) AND exists(b.AGE) AND abs(a.AGE - b.AGE)< 5 AND b.gender = a.gender AND b.pets = 'pes' AND b.children = 'mam'
RETURN b