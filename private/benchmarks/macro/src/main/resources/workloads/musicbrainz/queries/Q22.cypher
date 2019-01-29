MATCH (al:Album)
WHERE (:Artist)-->(al) AND (al)<-[:PART_OF]-(:Release)
RETURN *
LIMIT 50