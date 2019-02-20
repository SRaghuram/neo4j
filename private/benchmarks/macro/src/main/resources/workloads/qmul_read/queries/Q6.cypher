MATCH (a:Person)
  WHERE exists(a.gender)
RETURN count(a)
