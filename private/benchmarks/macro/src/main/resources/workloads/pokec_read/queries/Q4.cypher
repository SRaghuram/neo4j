MATCH (s:PROFILES { _key: { key }})-->(x)
MATCH (x)-->(n:PROFILES)
RETURN DISTINCT n._key