MATCH (s:PROFILES { _key: $key })-->(x)-->(n:PROFILES)
WHERE NOT (s)-->(n)
RETURN count(*)