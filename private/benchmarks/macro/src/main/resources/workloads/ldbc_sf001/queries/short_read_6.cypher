MATCH (post:Post)<-[:REPLY_OF_POST|REPLY_OF_COMMENT*0..]-(:Message {id:{Message}}),
      (moderator)<-[:HAS_MODERATOR]-(forum)-[:CONTAINER_OF]->(post)
RETURN
 forum.id AS forumId,
 forum.title AS forumTitle,
 moderator.id AS moderatorId,
 moderator.firstName AS moderatorFirstName,
 moderator.lastName AS moderatorLastName
LIMIT 1
