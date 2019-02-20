MATCH (person:Person {id:{1}})-[:KNOWS*1..2]-(friend)
WHERE NOT person=friend
WITH DISTINCT friend
MATCH (friend)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)
WHERE message.creationDate < {2}
WITH friend, message
ORDER BY message.creationDate DESC, message.id ASC
LIMIT {3}
RETURN message.id AS messageId,
       coalesce(message.content,message.imageFile) AS messageContent,
       message.creationDate AS messageCreationDate,
       friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName
