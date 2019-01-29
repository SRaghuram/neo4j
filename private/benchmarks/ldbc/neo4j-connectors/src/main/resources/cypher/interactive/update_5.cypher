MATCH (person:Person {id:{2}}), (forum:Forum {id:{1}})
CREATE (forum)-[:HAS_MEMBER {joinDate:{3}}]->(person)
