MATCH (a:Song)-[r]-(b)
WITH r,b
LIMIT 10000
RETURN type(r), labels(b), count(*)
ORDER BY count(*) DESC