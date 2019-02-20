MATCH (a:Album)
  WHERE (a.releasedIn = 1989 AND a.title = 'Album-5') OR (a.releasedIn = 2000)
RETURN *
