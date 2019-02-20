MATCH (subject:User { name: { name }})
MATCH p=(subject)-[:WORKED_WITH*0..1]-()-[:WORKED_WITH]-(person)-[:INTERESTED_IN]->(interest)
WHERE person<>subject AND interest.name IN { interests }
WITH person, interest, min(length(p)) AS pathLength
RETURN person.name AS name, count(interest) AS score, collect(interest.name) AS interests,(pathLength - 1) AS distance
ORDER BY score DESC LIMIT 20