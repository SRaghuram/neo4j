MATCH (a:User { name: { name }})-[:INTERESTED_IN]->(t:Topic)<-[:INTERESTED_IN]-(b:User)
OPTIONAL MATCH (a)-[:WORKED_WITH]->(b)
RETURN count(*)