MATCH (person:Person {id:{1}})-[:KNOWS]-(friend),
      (friend)<-[:POST_HAS_CREATOR]-(post)-[:POST_HAS_TAG]->(tag)
WITH DISTINCT tag, post
WITH tag,
     CASE
       WHEN {3} > post.creationDate >= {2} THEN 1
       ELSE 0
     END AS valid,
     CASE
       WHEN {2} > post.creationDate THEN 1
       ELSE 0
     END AS inValid
WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount>0 AND inValidPostCount=0
RETURN tag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT {4}
