MATCH (message:Message {id:{1}})-[:COMMENT_HAS_CREATOR|POST_HAS_CREATOR]->(person)
RETURN
 person.id AS personId,
 person.firstName AS personFirstName,
 person.lastName AS personLastName
