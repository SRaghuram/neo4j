MATCH (n:PROFILES)
WHERE n.children STARTS WITH ""
RETURN n.children, count(*)
