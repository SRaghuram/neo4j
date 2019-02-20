MATCH (person:Person {id:{1}})-[:KNOWS*2..2]-(friend),
       (friend)-[:PERSON_IS_LOCATED_IN]->(city)
WHERE NOT friend=person AND
      NOT (friend)-[:KNOWS]-(person) AND
      ( (friend.birthday_month={2} AND friend.birthday_day>=21) OR
        (friend.birthday_month=({2}%12)+1 AND friend.birthday_day<22) )
WITH DISTINCT friend, city, person
OPTIONAL MATCH (friend)<-[:POST_HAS_CREATOR]-(post)
WITH friend, city, collect(post) AS posts, person
WITH friend,
     city,
     length(posts) AS postCount,
     length([p IN posts WHERE (p)-[:POST_HAS_TAG]->()<-[:HAS_INTEREST]-(person)]) AS commonPostCount
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       friend.gender AS personGender,
       city.name AS personCityName,
       commonPostCount - (postCount - commonPostCount) AS commonInterestScore
ORDER BY commonInterestScore DESC, personId ASC
LIMIT {4}
