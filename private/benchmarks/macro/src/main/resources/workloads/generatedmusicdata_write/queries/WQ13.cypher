MATCH (c:Concert { year: { year }})-[IN :IN]->(v:Venue)
WITH c, v, IN LIMIT 10
OPTIONAL MATCH (c)<-[r]-(n)
MERGE (y:ConcertYear { year:c.year })
MERGE (y)<-[:FOR]-(v)
MERGE (y)<-[:PERFORMED_DURING]-(n)
DELETE IN , r, c