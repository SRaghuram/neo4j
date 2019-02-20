MATCH (a:Artist)
WHERE a:Band OR a:Person
REMOVE a:Artist