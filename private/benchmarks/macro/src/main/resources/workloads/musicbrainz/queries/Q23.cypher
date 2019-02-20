MATCH (artist:Artist)
WHERE NOT (artist)-->(:Album)
RETURN *
LIMIT 50