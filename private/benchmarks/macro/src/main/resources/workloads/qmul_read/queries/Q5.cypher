MATCH (a:Person)
  WHERE exists(a.email)
RETURN count(a)
