MATCH (a:Artist)
WHERE a:Band OR a:Person
RETURN count(a)