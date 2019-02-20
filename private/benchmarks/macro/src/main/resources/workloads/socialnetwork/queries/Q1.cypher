MATCH (subject:User { name: { name }})
MATCH p=(subject)-[:WORKED_ON]->()<-[:WORKED_ON]-(person)-[:INTERESTED_IN]->(interest)
WHERE person<>subject AND interest.name= { topic }
WITH DISTINCT person.name AS name, min(length(p)) AS pathLength
ORDER BY pathLength ASC LIMIT 10
RETURN name, pathLength
UNION
MATCH (subject:User { name: { name }})
MATCH p=(subject)-[:WORKED_ON]->()-[:WORKED_ON]-()<-[:WORKED_ON]-(person)-[:INTERESTED_IN]->(interest)
WHERE person<>subject AND interest.name= { topic }
WITH DISTINCT person.name AS name, min(length(p)) AS pathLength
ORDER BY pathLength ASC LIMIT 10
RETURN name, pathLength
UNION
MATCH (subject:User { name: { name }})
MATCH p=(subject)-[:WORKED_ON]->()-[:WORKED_ON]-()-[:WORKED_ON]-()<-[:WORKED_ON]-(person)-[:INTERESTED_IN]->(interest)
WHERE person<>subject AND interest.name= { topic }
WITH DISTINCT person.name AS name, min(length(p)) AS pathLength
ORDER BY pathLength ASC LIMIT 10
RETURN name, pathLength