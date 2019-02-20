MATCH (p:PROFILES)-->(n)
WHERE p._key=n._key AND n._key = 1
RETURN n._key