MATCH (a:Person)
  WHERE a.gender > 'female'
RETURN count(a)
