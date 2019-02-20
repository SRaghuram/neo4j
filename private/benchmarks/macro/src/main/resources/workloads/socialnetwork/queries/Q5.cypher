MATCH (person:User { name: { name }})
MATCH (person)-[:INTERESTED_IN]->()<-[:INTERESTED_IN]-(colleague)-[:INTERESTED_IN]->(topic)
WHERE topic.name= { topic }
WITH colleague
MATCH (colleague)-[:INTERESTED_IN]->(allTopics)
RETURN colleague.name AS name, collect(DISTINCT (allTopics.name)) AS topics