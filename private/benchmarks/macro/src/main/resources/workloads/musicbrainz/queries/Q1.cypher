MATCH (a:Artist)
WITH a
ORDER BY a.name
LIMIT 1000
MATCH (a)-[r]-(b)
RETURN type(r), labels(b), count(*)
ORDER BY count(*) DESC