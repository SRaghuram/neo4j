MATCH
  (:Country {name: $country})<-[:IS_PART_OF]-(city:City),
  (city)<-[:PERSON_IS_LOCATED_IN]-(person:Person),
  (person)<-[:HAS_MODERATOR]-(forum:Forum)-[:CONTAINER_OF]->(post:Post),
  (post)-[:POST_HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
RETURN
  forum.id,
  forum.title,
  forum.creationDate,
  person.id,
  count(DISTINCT post) AS postCount
ORDER BY
  postCount DESC,
  forum.id ASC
LIMIT 20
