MATCH (person:Person {id:{1}}), (comment:Message {id:{2}})
CREATE (person)-[:LIKES_COMMENT {creationDate:{3}}]->(comment)
