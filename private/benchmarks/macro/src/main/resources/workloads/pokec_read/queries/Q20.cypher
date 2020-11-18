MATCH (a:PROFILES { _key: $key }),(b:PROFILES)
WHERE a.AGE IS NOT NULL AND b.AGE IS NOT NULL AND abs(a.AGE - b.AGE)< 5 AND b.gender = a.gender AND b.hair_color = a.hair_color AND b.eye_color = a.eye_color
RETURN b