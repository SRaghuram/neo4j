MATCH (a:Person)
  WHERE a.email <= 'alextange@me.com'
RETURN count(a)
