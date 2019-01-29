MATCH (person:Person {id:{1}}), (post:Message {id:{2}})
CREATE (person)-[:LIKES_POST {creationDate:{3}}]->(post)
