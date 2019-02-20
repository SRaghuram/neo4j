MATCH (author)<-[:COMMENT_HAS_CREATOR|POST_HAS_CREATOR]-(message:Message {id:{1}}),
      (message)<-[:REPLY_OF_COMMENT|REPLY_OF_POST]-(reply),
      (reply)-[:COMMENT_HAS_CREATOR]->(replyAuthor)
RETURN
 replyAuthor.id AS replyAuthorId,
 replyAuthor.firstName AS replyAuthorFirstName,
 replyAuthor.lastName AS replyAuthorLastName,
 reply.id AS replyId,
 reply.content AS replyContent,
 reply.creationDate AS replyCreationDate,
 exists((author)-[:KNOWS]-(replyAuthor)) AS replyAuthorKnowsAuthor
ORDER BY replyCreationDate DESC, replyAuthorId ASC
