MATCH (country:Country {name:{Country}})
MATCH (person:Person {id:{Person}})-[:KNOWS*1..2]-(friend)
WHERE NOT person=friend
WITH DISTINCT friend, country
MATCH (friend)-[worksAt:WORKS_AT]->(company)-[:ORGANISATION_IS_LOCATED_IN]->(country)
WHERE worksAt.workFrom<{Year}
RETURN
 friend.id AS friendId,
 friend.firstName AS friendFirstName,
 friend.lastName AS friendLastName,
 worksAt.workFrom AS workFromYear,
 company.name AS companyName
ORDER BY workFromYear ASC, friendId ASC, companyName DESC
LIMIT 20
