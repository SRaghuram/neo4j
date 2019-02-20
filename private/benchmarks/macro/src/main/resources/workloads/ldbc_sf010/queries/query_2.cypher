MATCH (:Person {id:{Person}})-[:KNOWS]-(friend),
      (friend)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)
WHERE message.creationDate <= {Date0}
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       message.id AS messageId,
       coalesce(message.content, message.imageFile) AS messageContent,
       message.creationDate AS messageDate
ORDER BY messageDate DESC, messageId ASC
LIMIT 20
