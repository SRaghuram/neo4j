MATCH (current:App { app_id: { app_id }})-[:DEFINED_BY_SERGE]->(tag:Tag)
WITH current, tag
MATCH (recommendation:App)
WHERE recommendation <> current
MATCH (recommendation)-[:DEFINED_BY_SERGE]->(tag)
WITH recommendation, count(*) AS freq
ORDER BY freq DESC LIMIT 10
RETURN recommendation.name AS Recommended, freq