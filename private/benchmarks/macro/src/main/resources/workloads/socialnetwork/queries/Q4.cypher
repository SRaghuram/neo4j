MATCH (subject:User { name: { name }})
MATCH (subject)-[:INTERESTED_IN]->(interest)<-[:INTERESTED_IN]-(person),(person)-[:WORKS_FOR]->(company)
RETURN person.name AS name, company.name AS company, count(interest) AS score, collect(interest.name) AS interests
ORDER BY score DESC