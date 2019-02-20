MATCH (subject:User { name: { name }})
MATCH (subject)-[:WORKS_FOR]->(company)<-[:WORKS_FOR]-(person),(subject)-[:INTERESTED_IN]->(interest)<-[:INTERESTED_IN]-(person)
RETURN person.name AS name, count(interest) AS score, collect(interest.name) AS interests
ORDER BY score DESC