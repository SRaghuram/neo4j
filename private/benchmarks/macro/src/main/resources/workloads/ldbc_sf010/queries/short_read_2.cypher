MATCH (:Person {id:{Person}})<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)
WITH
 message,
 message.id AS messageId,
 message.creationDate AS messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT 20
MATCH (message)-[:REPLY_OF_COMMENT|REPLY_OF_POST*0..]->(post:Post),
      (post)-[:POST_HAS_CREATOR]->(person)
RETURN
 messageId,
 messageCreationDate,
 coalesce(message.imageFile,message.content) AS messageContent,
 post.id AS postId,
 person.id AS personId,
 person.firstName AS personFirstName,
 person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC
