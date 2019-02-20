MATCH (person1:Person {id:{1}}), (person2:Person {id:{2}})
CREATE (person1)-[:KNOWS {creationDate:{3}}]->(person2)
