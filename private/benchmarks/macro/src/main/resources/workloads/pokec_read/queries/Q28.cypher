MATCH (a:PROFILES { _key: { key }}),(b:PROFILES)
WHERE exists(a.AGE) AND exists(b.AGE) AND abs(a.AGE - b.AGE)< 5 AND b.gender = 0 AND b.hair_color = 'hnede' AND b.eye_color = 'hnede' AND b.pets = 'pes' AND b.children = 'nemam'
RETURN b