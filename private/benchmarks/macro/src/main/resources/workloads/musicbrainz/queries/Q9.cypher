MATCH (a:Artist { name: { name }})-[r]-(b)
RETURN type(r), labels(b), count(*)
ORDER BY count(*) DESC LIMIT 25