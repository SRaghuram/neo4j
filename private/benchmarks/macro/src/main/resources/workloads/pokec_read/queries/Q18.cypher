MATCH (a:PROFILES { _key: { key }}),(b:PROFILES)
WHERE b.hair_color = a.hair_color
RETURN b