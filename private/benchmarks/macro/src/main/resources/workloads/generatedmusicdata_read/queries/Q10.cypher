MATCH (a:Album)
  WHERE (a.releasedIn = 1989 OR a.title = 'Album-5')
RETURN *
