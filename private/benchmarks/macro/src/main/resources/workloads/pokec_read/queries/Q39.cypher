MATCH (s:PROFILES { _key: $_key })-->(x)-->(n:PROFILES)
WHERE NOT (s)-->(n)
RETURN count(*)