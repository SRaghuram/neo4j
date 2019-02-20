MATCH (al:Album), (a:Artist)
  WHERE al.title = a.name
RETURN *
