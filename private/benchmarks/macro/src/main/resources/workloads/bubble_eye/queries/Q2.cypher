MATCH (current:App { app_id: { app_id }})-[:DEFINED_BY_SERGE]->(tag:Tag)<-[:DEFINED_BY_SERGE]-(recommendation:App)
WITH recommendation, count(*) AS freq
ORDER BY freq DESC LIMIT 10
RETURN recommendation.name AS Recommended, freq