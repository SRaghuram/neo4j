MATCH (person:Person {id:{Person}})-[:KNOWS*1..2]-(friend)
WHERE NOT person=friend
WITH DISTINCT friend
MATCH (friend)<-[membership:HAS_MEMBER]-(forum)
WHERE membership.joinDate>{Date0}
WITH forum, collect(friend) AS friends
OPTIONAL MATCH (friend)<-[:POST_HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
WHERE friend IN friends
WITH forum, count(post) AS postCount
RETURN forum.title AS forumName, postCount
ORDER BY postCount DESC, forum.id ASC
LIMIT 20
