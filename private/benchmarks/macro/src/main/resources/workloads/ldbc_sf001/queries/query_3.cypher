MATCH (countryX:Country {name:{Country1}}),
      (countryY:Country{name:{Country2}}),
      (person:Person {id:{Person}})
WITH person, countryX, countryY
LIMIT 1
MATCH (city:City)-[:IS_PART_OF]->(country:Country)
WHERE country IN [countryX, countryY]
WITH person, countryX, countryY, collect(city) AS cities
MATCH (person)-[:KNOWS*1..2]-(friend)-[:PERSON_IS_LOCATED_IN]->(city)
WHERE NOT person=friend AND NOT city IN cities
WITH DISTINCT friend, countryX, countryY
MATCH (friend)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message),
      (message)-[:POST_IS_LOCATED_IN|COMMENT_IS_LOCATED_IN]->(country)
WHERE {Date0}+({Duration}*24*60*60*1000)>message.creationDate>={Date0} AND
      country IN [countryX, countryY]
WITH friend,
     CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
     CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
WHERE xCount>0 AND yCount>0
RETURN friend.id AS friendId,
       friend.firstName AS friendFirstName,
       friend.lastName AS friendLastName,
       xCount,
       yCount,
       xCount + yCount AS xyCount
ORDER BY xyCount DESC, friendId ASC
LIMIT 20
