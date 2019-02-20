MATCH (moderator:Person {id:{2}})
OPTIONAL MATCH (tag:Tag) WHERE tag.id IN {3}
WITH moderator, collect(tag) AS tags
CREATE (forum:Forum {1})-[:HAS_MODERATOR]->(moderator)
FOREACH (tag IN tags | CREATE (forum)-[:FORUM_HAS_TAG]->(tag))
