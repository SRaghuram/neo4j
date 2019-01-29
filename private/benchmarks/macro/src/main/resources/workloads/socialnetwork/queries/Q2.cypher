MATCH (subject:User { name: { name }})
MATCH p=(subject)-[:WORKED_ON]->()<-[:WORKED_ON]-(person)-[:INTERESTED_IN]->(interest)
WHERE person<>subject AND interest.name IN { interests }
WITH person, interest, min(length(p)) AS pathLength
ORDER BY interest.name
RETURN person.name AS name, count(interest) AS score, collect(interest.name) AS interests,((pathLength - 1)/2) AS distance
ORDER BY score DESC LIMIT 10
UNION
MATCH (subject:User { name: { name }})
MATCH p=(subject)-[:WORKED_ON]->()-[:WORKED_ON]-()<-[:WORKED_ON]-(person)-[:INTERESTED_IN]->(interest)
WHERE person<>subject AND interest.name IN { interests }
WITH person, interest, min(length(p)) AS pathLength
ORDER BY interest.name
RETURN person.name AS name, count(interest) AS score, collect(interest.name) AS interests,((pathLength - 1)/2) AS distance
ORDER BY score DESC LIMIT 10
UNION
MATCH (subject:User { name: { name }})
MATCH p=(subject)-[:WORKED_ON]->()-[:WORKED_ON]-()-[:WORKED_ON]-()<-[:WORKED_ON]-(person)-[:INTERESTED_IN]->(interest)
WHERE person<>subject AND interest.name IN { interests }
WITH person, interest, min(length(p)) AS pathLength
ORDER BY interest.name
RETURN person.name AS name, count(interest) AS score, collect(interest.name) AS interests,((pathLength - 1)/2) AS distance
ORDER BY score DESC LIMIT 10