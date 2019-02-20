MATCH p=(n:Person { first_name: { first_name }})-[:FRIEND_OF]-(m:Person)
RETURN p
UNION
MATCH p=(n:Person { first_name: { first_name }})-[:FRIEND_OF]-()-[:FRIEND_OF]-(m:Person)
RETURN p