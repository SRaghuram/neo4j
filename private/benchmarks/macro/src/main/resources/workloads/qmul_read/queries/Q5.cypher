MATCH (a:Person)
  WHERE a.email IS NOT NULL
RETURN count(a)
