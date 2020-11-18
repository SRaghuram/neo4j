MATCH (a:Person)
  WHERE a.gender IS NOT NULL
RETURN count(a)
