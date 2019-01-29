MATCH (a:PROFILES { _key: { key }}),(b:PROFILES)
WHERE exists(a.AGE) AND exists(b.AGE) AND abs(a.AGE - b.AGE)< 5 AND b.gender = 1 AND b.hair_color = 'blond'
RETURN b