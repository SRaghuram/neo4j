MATCH (n:Person)
  WHERE n.number < 0
RETURN count(n)
