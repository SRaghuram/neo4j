MATCH (a:Album)
  WHERE a.title = {title} OR a.releasedIn = {releasedIn}
RETURN *
