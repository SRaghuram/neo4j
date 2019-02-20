MATCH (knownTag:Tag {name:{Tag}})
MATCH (person:Person {id:{Person}})-[:KNOWS*1..2]-(friend)
WHERE NOT person=friend
WITH DISTINCT friend, knownTag
MATCH (friend)<-[:POST_HAS_CREATOR]-(post)
WHERE (post)-[:POST_HAS_TAG]->(knownTag)
WITH post, knownTag
MATCH (post)-[:POST_HAS_TAG]->(commonTag)
WHERE NOT commonTag=knownTag
WITH commonTag, count(post) AS postCount
RETURN commonTag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 20
