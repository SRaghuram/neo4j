MATCH (person:Person {id:$Person})-[:KNOWS*2..2]-(friend),
       (friend)-[:PERSON_IS_LOCATED_IN]->(city)
WHERE NOT friend=person AND
      NOT (friend)-[:KNOWS]-(person) AND
      ( (friend.birthday_month=$HS0 AND friend.birthday_day>=21) OR
        (friend.birthday_month=($HS0%12)+1 AND friend.birthday_day<22) )
WITH DISTINCT friend, city, person
CALL {
  WITH friend, person
  OPTIONAL MATCH (friend)<-[:POST_HAS_CREATOR]-(post)
  CALL {
    WITH post, person
    WITH *
    WHERE (post)-[:POST_HAS_TAG]->()<-[:HAS_INTEREST]-(person)
    RETURN count(post) AS commonPost
  }
  RETURN count(post) AS postCount,
         sum(commonPost) AS commonPostCount
}
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       friend.gender AS personGender,
       city.name AS personCityName,
       commonPostCount - (postCount - commonPostCount) AS commonInterestScore
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 20
