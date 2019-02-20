MATCH (person:Person {id:{Person}})<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message),
      (message)<-[like:LIKES_POST|LIKES_COMMENT]-(liker)
WITH liker, message, like.creationDate AS likeTime, person
ORDER BY likeTime DESC, message.id ASC
WITH liker,
     head(collect(message)) AS message,
     head(collect(likeTime)) AS likeTime,
     person
RETURN
 liker.id AS personId,
 liker.firstName AS personFirstName,
 liker.lastName AS personLastName,
 likeTime,
 not((liker)-[:KNOWS]-(person)) AS isNew,
 message.id AS messageId,
 coalesce(message.content,message.imageFile) AS messageContent,
 message.creationDate AS messageCreationDate
ORDER BY likeTime DESC, personId ASC
LIMIT 20
