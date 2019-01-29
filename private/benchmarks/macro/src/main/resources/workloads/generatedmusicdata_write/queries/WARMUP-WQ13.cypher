MATCH (c:Concert { year: { year }})-[IN :IN]->(v:Venue)
WITH c, v
LIMIT 10
OPTIONAL MATCH (c)<-[r]-(n)
RETURN count(n)