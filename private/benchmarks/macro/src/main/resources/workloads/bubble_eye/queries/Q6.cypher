MATCH (current:App { app_id: { app_id }})-[:DEFINED_BY_SERGE]->(tag:Tag)
WITH count(*) AS tagCount, collect(tag) AS tags
MATCH (recommendation:App)
WITH recommendation, size((recommendation)-[:DEFINED_BY_SERGE]->()) AS degree,tags
ORDER BY degree DESC LIMIT 10000
RETURN id(recommendation), SIZE(FILTER(t IN tags WHERE (recommendation)-[:DEFINED_BY_SERGE]->(t))) AS freq
ORDER BY freq DESC LIMIT 10