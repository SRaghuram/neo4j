MATCH (tag:Tag {name: $tag})<-[:COMMENT_HAS_TAG|POST_HAS_TAG]-(message:Message),
      (message)-[:COMMENT_HAS_CREATOR|POST_HAS_CREATOR]->(person:Person)
OPTIONAL MATCH (:Person)-[like:LIKES_COMMENT|LIKES_POST]->(message)
OPTIONAL MATCH (message)<-[:REPLY_OF_COMMENT|REPLY_OF_POST]-(comment:Comment)
WITH person,
     count(DISTINCT like) AS likeCount,
     count(DISTINCT comment) AS replyCount,
     count(DISTINCT message) AS messageCount
RETURN
  person.id,
  replyCount,
  likeCount,
  messageCount,
  1*messageCount + 2*replyCount + 10*likeCount AS score
ORDER BY
  score DESC,
  person.id ASC
LIMIT 100
