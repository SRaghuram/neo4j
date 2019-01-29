MATCH (person:Person {id:{2}}), (country:Country {id:{3}}), (message :Message {id:{4}})
OPTIONAL MATCH (tag:Tag) WHERE tag.id IN {6}
WITH person, country, message, collect(tag) AS tags
CREATE (comment:Comment:Message {1})
CREATE (comment)-[:COMMENT_HAS_CREATOR]->(person)
CREATE (comment)-[:COMMENT_IS_LOCATED_IN]->(country)
FOREACH (flag IN [f IN [{5}] WHERE f=true] | CREATE (comment)-[:REPLY_OF_POST]->(message))
FOREACH (flag IN [f IN [{5}] WHERE f=false] | CREATE (comment)-[:REPLY_OF_COMMENT]->(message))
FOREACH (tag IN tags | CREATE (comment)-[:COMMENT_HAS_TAG]->(tag))
