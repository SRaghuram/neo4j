MATCH (person:Person {id:{2}}), (forum:Forum {id:{3}}), (country:Country {id:{4}})
OPTIONAL MATCH (tag:Tag) WHERE tag.id IN {5}
WITH person, forum, country, collect(tag) AS tags
CREATE (forum)-[:CONTAINER_OF]->(post:Post:Message {1})-[:POST_HAS_CREATOR]->(person), (post)-[:POST_IS_LOCATED_IN]->(country)
FOREACH (tag IN tags | CREATE (post)-[:POST_HAS_TAG]->(tag))
