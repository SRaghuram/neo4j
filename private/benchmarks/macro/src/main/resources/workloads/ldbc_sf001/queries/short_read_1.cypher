MATCH (person:Person {id:{Person}})-[:PERSON_IS_LOCATED_IN]->(city)
RETURN
 person.firstName AS firstName,
 person.lastName AS lastName,
 person.birthday AS birthday,
 person.locationIP AS locationIp,
 person.browserUsed AS browserUsed,
 person.gender AS gender,
 person.creationDate AS creationDate,
 city.id AS cityId
