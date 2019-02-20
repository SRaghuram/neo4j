MATCH (start:Person {id:{Person}})<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message),
      (message)<-[:REPLY_OF_POST|REPLY_OF_COMMENT]-(comment)-[:COMMENT_HAS_CREATOR]->(person)
RETURN
 person.id AS personId,
 person.firstName AS personFirstName,
 person.lastName AS personLastName,
 comment.id AS commentId,
 comment.creationDate AS commentCreationDate,
 comment.content AS commentContent
ORDER BY commentCreationDate DESC, commentId ASC
LIMIT 20
