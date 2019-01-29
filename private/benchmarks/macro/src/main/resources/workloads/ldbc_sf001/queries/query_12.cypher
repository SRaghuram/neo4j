MATCH (:Person {id:{Person}})-[:KNOWS]-(friend:Person),
      (friend)<-[:COMMENT_HAS_CREATOR]-(comment:Comment),
      (comment)-[:REPLY_OF_POST]->(post:Post),
      (post)-[:POST_HAS_TAG]->(tag:Tag),
      (tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(:TagClass{name:{TagType}})
RETURN
 friend.id AS friendId,
 friend.firstName AS friendFirstName,
 friend.lastName AS friendLastName,
 collect(DISTINCT tag.name) AS tagNames,
 count(DISTINCT comment) AS count
ORDER BY count DESC, friendId ASC
LIMIT 20
