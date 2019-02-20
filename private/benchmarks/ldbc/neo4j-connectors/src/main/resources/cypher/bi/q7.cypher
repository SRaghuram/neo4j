MATCH (tag:Tag {name: $tag})
MATCH (tag)<-[:COMMENT_HAS_TAG|POST_HAS_TAG]-(message1:Message)-[:COMMENT_HAS_CREATOR|POST_HAS_CREATOR]->(person1:Person)
MATCH (tag)<-[:COMMENT_HAS_TAG|POST_HAS_TAG]-(message2:Message)-[:COMMENT_HAS_CREATOR|POST_HAS_CREATOR]->(person1)
OPTIONAL MATCH (message2)<-[:LIKES_COMMENT|LIKES_POST]-(person2:Person)
OPTIONAL MATCH (person2)<-[:COMMENT_HAS_CREATOR|POST_HAS_CREATOR]-(message3:Message)<-[like:LIKES_COMMENT|LIKES_POST]-(p3:Person)
RETURN
  person1.id,
  count(DISTINCT like) AS authorityScore
ORDER BY
  authorityScore DESC,
  person1.id ASC
LIMIT 100
