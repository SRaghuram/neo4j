MATCH (a:User { name: { name }})-[:INTERESTED_IN]->(t:Topic)<-[:INTERESTED_IN]-(b:User)
MATCH (a)-[:WORKED_WITH]->(b)
RETURN count(*)