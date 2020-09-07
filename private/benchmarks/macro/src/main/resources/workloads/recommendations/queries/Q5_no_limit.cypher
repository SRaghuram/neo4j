MATCH path1=(p1:Person)-[:FRIEND_OF]-()-[:FRIEND_OF]-(p2:Person)
WHERE NOT (p1)-[:FRIEND_OF]-(p2)
RETURN path1