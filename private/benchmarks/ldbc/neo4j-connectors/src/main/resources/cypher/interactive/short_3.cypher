MATCH (:Person {id:{1}})-[knows:KNOWS]-(friend)
RETURN
 friend.id AS friendId,
 friend.firstName AS friendFirstName,
 friend.lastName AS friendLastName,
 knows.creationDate AS knowsCreationDate
ORDER BY knows.creationDate DESC, friend.id ASC
