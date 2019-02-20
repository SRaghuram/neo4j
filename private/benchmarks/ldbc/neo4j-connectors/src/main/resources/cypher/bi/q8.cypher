MATCH
  (tag:Tag {name: $tag})<-[:COMMENT_HAS_TAG|POST_HAS_TAG]-(message:Message),
  (message)<-[:REPLY_OF_COMMENT|REPLY_OF_POST]-(comment:Comment)-[:COMMENT_HAS_TAG]->(relatedTag:Tag)
WHERE NOT (comment)-[:COMMENT_HAS_TAG]->(tag)
RETURN
  relatedTag.name,
  count(DISTINCT comment) AS count
ORDER BY
  count DESC,
  relatedTag.name ASC
LIMIT 100
