MATCH
  (country:Country)<-[:IS_PART_OF]-(:City)<-[:PERSON_IS_LOCATED_IN]-(person:Person),
  (person)<-[:COMMENT_HAS_CREATOR|POST_HAS_CREATOR]-(message:Message),
  (message)-[:COMMENT_HAS_TAG|POST_HAS_TAG]->(tag:Tag)
WHERE message.creationDate >= $startDate
  AND message.creationDate <= $endDate
  AND (country.name = $country1 OR country.name = $country2)
WITH
  country.name AS countryName,
  message.creationDate/100000000000%100 AS month,
  person.gender AS gender,
  floor((20130101 - person.birthday) / 10000 / 5.0) AS ageGroup,
  tag.name AS tagName,
  message
WITH
  countryName, month, gender, ageGroup, tagName, count(message) AS messageCount
WHERE messageCount > 100
RETURN
  countryName,
  month,
  gender,
  ageGroup,
  tagName,
  messageCount
ORDER BY
  messageCount DESC,
  tagName ASC,
  ageGroup ASC,
  gender ASC,
  month ASC,
  countryName ASC
LIMIT 100
