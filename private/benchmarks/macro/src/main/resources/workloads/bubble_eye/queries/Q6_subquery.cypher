MATCH (current:App { app_id: $app_id })-[:DEFINED_BY_SERGE]->(tag:Tag)
WITH count(*) AS tagCount, collect(tag) AS tags
MATCH (recommendation:App)
CALL {
  WITH recommendation
  MATCH (recommendation)-[:DEFINED_BY_SERGE]->()
  RETURN count(*) AS degree
}
WITH recommendation, degree, tags
ORDER BY degree DESC LIMIT 10000
CALL {
  WITH recommendation, tags
  UNWIND tags AS tag
  WITH recommendation, tag
  WHERE (recommendation)-[:DEFINED_BY_SERGE]->(tag)
  RETURN count(*) AS freq
}
RETURN id(recommendation), freq
ORDER BY freq DESC LIMIT 10
